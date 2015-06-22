import collections
from ..aux import *
from ..queue import *
from ..core import *
from ..streams import InterruptedTransfer
from .framing import *
from .errors import *
import traceback
from time import time

class WebsocketClosed(Exception):
	pass

(BUILD_NOTHING, BUILD_TEXT, BUILD_BINARY) = range(3)

(RECV_PING, RECV_PONG, RECV_CLOSE, RECV_START_TEXT,
 RECV_START_BINARY, RECV_CONTINUE, RECV_FINAL_FRAGMENT,
 RECV_SINGLE_TEXT, RECV_SINGLE_BINARY) = range(9)

(STATE_OPEN, STATE_CLOSING, STATE_CLOSED, STATE_ERROR) = range(4)

(CLOSE_BY_ERROR, CLOSE_BY_LOCAL, CLOSE_BY_REMOTE,
 CLOSE_BY_LOCAL_TIMEOUT)                          = range(4)

class Websocket:
	""" An implementation of the websocket standard.
	
	    This is the basic class for a websocket. It is wrapped around an
	    UnstuckSocket once the opening handshake has cleared. This class will
	    not perform the handshake itself but relies on this having been done
	    prior to wrapping. Once wrapped a websocket cannot be released without
	    closing the socket that has been wrapped.
	"""
	def __init__(self, socket, receiveMask = True, sendMask = False,
	                   queueLength = 10):
		self.socket = socket
		
		# If a websocket somehow closes without any websocket close event
		# (e.g. by disruption of transport) then the data below will be
		# used for the closure event
		self.closingData = CLOSE_BY_ERROR, None, None
		
		# Mapping of ping IDs to waiters, in chronological order.
		self.pings = collections.OrderedDict()
		
		# Buffer for received data packets
		self.dataFrameQueue = XQueue(queueLength)
		
		# These specify whether the websocket sends data with masking enabled
				# and whether it expects masking on received data.
		self.receiveMask = receiveMask
		self.sendMask = sendMask
		
		# Maximum permitted packet size before packets are split.
		self.maxSize = 4096
		
		# Start the main loop.
		self.state = STATE_OPEN
		self.mainLoopThread = async(self.__mainLoop())
		
		# Write waiter
		self.curWait = doneFuture
	
	def __del__(self):
		# If the websocket is collected whilst open, then forceClose the
		# underlying socket, this should cause the main loop to exit.
		if self.state is STATE_OPEN:
			self.socket.forceClose()
	
	@asynchronous
	def recv(self):
		""" Receive one complete packet from the websocket.
		
		    This function receives one complete data packet from the websocket.
		    If a data packet is not currently available then it will block until
		    one is available. A packet can either be a `str' or `bytes' object
		    depending on whether the packet is a binary blob or a string packet.
		    If the websocket is closed or closing then an error is raised
		    instead.
		"""
		if self.state is STATE_OPEN:
			return self.dataFrameQueue.get()
		else:
			return errorFuture(WebsocketClosed(self.closingData))
	
	@asynchronous
	def send(self, data):
		""" Send one complete packet through the websocket.
		
		    This function sends one complete data packet to the websocket.
		    It will block until the packet has been transmitted (i.e. has
		    been sent down the actual low-level socket.
		    If the websocket is closed or closing then an error is raised
		    instead.
		"""
		if self.state is STATE_OPEN:
			waitOn = self.curWait
			self.curWait = myWait = Future()
			yield from waitOn
			result = yield from self.__writeDataFrame(data)
			myWait.setResult(None)
			return result
		else:
			raise WebsocketClosed(self.closingData)
	
	@asynchronous
	def close(self, timeout = 2, reason = (1000, "OK")):
		if self.state == STATE_OPEN:
			self.state = STATE_CLOSING
			yield from self.__sendClose(timeout, reason, CLOSE_BY_LOCAL)
		yield from self.mainLoopThread
		return self.closingData
	
	@asynchronous
	def __mainLoop(self):
		buildBits = []
		buildWhat = BUILD_NOTHING
		while self.state != STATE_CLOSED:
			try:
				recvCode, recvBit = yield from self.__readNextFrame(buildWhat)
				if recvCode == RECV_START_TEXT:
					buildWhat = BUILD_TEXT
					buildBits.append(recvBit)
				
				elif recvCode == RECV_START_BINARY:
					buildWhat = BUILD_BINARY
					buildBits.append(recvBit)
				
				elif recvCode == RECV_SINGLE_TEXT:
					yield from self.dataFrameQueue.putResult(recvBit.decode())
				
				elif recvCode == RECV_SINGLE_BINARY:
					yield from self.dataFrameQueue.putResult(recvBit)
				
				elif recvCode == RECV_CONTINUE:
					self.buildBits.append(recvBits)
				
				elif recvCode == RECV_FINAL_FRAGMENT:
					self.buildBits.append(recvBits)
					if buildWhat == BUILD_TEXT:
						build = "".join(bit.decode() for bit in buildBits)
					elif buildWhat == BUILD_BINARY:
						build = b"".join(buildBits)
					yield from self.dataFrameQueue.putResult(build)
					buildBits = []
					buildWhat = BUILD_NOTHING
			# Socket closed
			except (InterruptedTransfer, BrokenPipeError):
				self.socket.forceClose()
				break
			except WebSocketProtocolError:
				yield from self.__failConnection(1002)
			except UnicodeDecodeError:
				yield from self.__failConnection(1007)
			except PayloadTooBig:
				yield from self.__failConnection(1009)
			except Exception as e:
				yield from self.__failConnection(1011)
		
		if self.state != STATE_CLOSED:
			self.state = STATE_CLOSED
			yield from self.socket.close()
		
		while self.dataFrameQueue.sumSize() < 0:
			self.dataFrameQueue.putError(WebsocketClosed(self.closingData))
	
	@asynchronous
	def __readNextFrame(self, building):
		opcode, data, final = yield from readFragment(self.socket,
		                                   self.receiveMask, self.maxSize)
		# Answer pings with corresponding pongs
		if opcode == OP_PING:
			yield from self.writePongFrame(data)
			return RECV_PING, None
		
		# When pongs are received then match them up to pings that have been
		# sent. A pong will clear all pings sent up to that pong.
		if opcode == OP_PONG:
			# Acknowledge all pings up to the one matching this pong.
			while data in self.sentPings:
				self.sentPings.popitem(0).setResult(None)
			return RECV_PONG, None
		
		# When a close opcode is received, then
		if opcode == OP_CLOSE:
			closeCode, closeReason = closeData = parseCloseData(data)
			# If the remote end has initiated the closing then this end needs
			# to send its own before exiting the loop
			if self.state == STATE_OPEN:
				yield from self.__writeCloseFrame(data)
				self.closingData = (CLOSE_BY_REMOTE, None, closeData)
			else:
				self.closingData = (self.closingData[0], closeData,
				                    self.closingData[2])
			self.state = STATE_CLOSED
			yield from self.socket.close()
			return RECV_CLOSE, None
		
		# Data frame
		if building == BUILD_NOTHING:
			if opcode == OP_BINARY:
				# Single-fragment binary frame
				if final:
					return RECV_SINGLE_BINARY, data
				# Start of multi-fragment binary frame
				else:
					return RECV_START_BINARY, data
		
			elif opcode == OP_TEXT:
				# Single-fragment text frame
				if final:
					return RECV_SINGLE_TEXT, data
				# Start of multi-fragment text frame
				else:
					return RECV_START_TEXT, data
		
		elif opcode == OP_CONT:
			# Final fragment in a multi-fragment frame
			if final:
				return RECV_FINAL_FRAGMENT, data
			# Intermediate fragment in a multi-fragment frame
			else:
				return RECV_CONTINUE, data
		
		raise WebSocketProtocolError("Unexpected opcode")
	
	@asynchronous
	def __writeCloseFrame(self, data):
		return writeFragment(self.socket, self.sendMask, OP_CLOSE, data, True)
	
	@asynchronous
	def __writeDataFrame(self, data):
		if isinstance(data, str):
			data = data.encode('utf-8')
			opcode = OP_TEXT
		else:
			opcode = OP_BINARY
		if len(data) > self.maxSize:
			endFrag = self.maxSize
			yield from writeFragment(self.socket, self.sendMask, opcode,
			                         data[:endFrag], False)
			while len(data) > endFrag + self.maxSize:
				endFrag += self.maxSize
				startFrag = endFrag
				yield from writeFragment(self.socket, self.sendMask, OP_CONT,
				                         data[startFrag:endFrag], False)
			yield from writeFragment(self.socket, self.sendMask, OP_CONT,
			                         data[endFrag:], True)
		else:
			yield from writeFragment(self.socket, self.sendMask,
			                         opcode, data, True)
	
	@asynchronous
	def __failConnection(self, code=1011, reason=""):
		if self.state == STATE_OPEN:
			self.state = STATE_ERROR
			yield from self.__sendClose(0.5, (code, reason), CLOSE_BY_ERROR)
	
	@asynchronous
	def __sendClose(self, timeout, reason, closeBy):
		self.closingData = (closeBy, None, reason)
		try:
			data = struct.pack("!H", reason[0]) + reason[1].encode()
			yield from self.__writeCloseFrame(data)
			dispatcher.scheduleHandle(time() + timeout, self.__closeTimeout)
		except (InterruptedTransfer, BrokenPipeError):
			self.state = STATE_ERROR
			self.__closeTimeout()
	
	def __closeTimeout(self):
		if self.state != STATE_CLOSED:
			self.closingData = (CLOSE_BY_LOCAL_TIMEOUT, None,
			                    self.closingData[2])
			self.state = STATE_CLOSED
			self.socket.forceClose()
			while self.dataFrameQueue.sumSize() > 0:
				try:
					self.dataFrameQueue.get()
				except:
					pass


def websocketServer(port, handler, backlog = 3):
	listener = USocket()
	listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	listener.bind(("",port))
	listener.listen(backlog)
	while True:
		socket = await(listener.accept())
		path, _ = await(serverHandshake(socket))
		websocket = Websocket(socket)
		async(handler(websocket, path))

