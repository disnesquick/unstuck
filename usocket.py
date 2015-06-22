import socket
import select
import errno
import os

# Local imports
from .core import *
from .aux import *
from .queue import *
from .streams import *

# Exports
__all__ = ["USocket"]

# Globals
(STATE_OPEN, STATE_CLOSING, STATE_CLOSED, 
 STATE_CONNECTED, STATE_LISTENING) = range(5)

class USocket:
	""" A wrapper class for asynchronous sockets.
	
	    This class tries to provide the same interface as the standard socket
	    object as found in the Python socket module. Some of those functions
	    have been made asynchronous, where possible. This class will delegate
	    class instantiation to a sub-class, for example a SOCK_STREAM will
	    actually be instantiated as a _USocketStream. This is because datagram
	    sockets and streaming sockets have substantially different interfaces
	    and OOP should reflect that.
	"""
	def __new__(cls, family = socket.AF_INET, type = socket.SOCK_STREAM,
	                 proto = 0, *, innerSocket = None):
		""" Object creation delegation for USocket.
		
		    This factory can be called in two ways, either the standard
		    arguments for a "normal" socket can be provided or the keyword
		    argument innerSocket can be used to provide an extant python
		    socket object. In the first case, a python socket will be created
		    and wrapped, in the second case, the provided object will be wrapped
		    The second case is only intended for internal use.
		"""
		if innerSocket is not None:
			return super().__new__(cls)
		elif type is socket.SOCK_STREAM:
			return super().__new__(_USocketStream)
		else:
			raise(NotImplemented)
		
	def __init__(self, family = socket.AF_INET, type = socket.SOCK_STREAM,
	                   proto = 0, innerSocket = None):
		# Initialization from a provided socket object
		if innerSocket is not None:
			self.socket = innerSocket
		
		# Initialization de-novo
		else:
			self.socket = socket.socket(family, type, proto)
			self.state = STATE_OPEN
		
		# Set non-blocking
		self.socket.settimeout(0)
	
	def __del__(self):
		if self.state != STATE_CLOSED:
			print("CLOSING")
			self.forceClose()
	
	def setsockopt(self, *args):
		""" Pass-through method for the underlying socket
		"""
		return self.socket.setsockopt(*args)
	
	def bind(self, address):
		""" Pass-through method for the underlying socket
		"""
		return self.socket.bind(address)
	
	@classmethod
	def listener(self, address, backlog = 1):
		listener = USocket()
		listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		listener.bind(address)
		listener.listen(backlog)
		return listener
		


class _USocketStream(USocket):
	""" Unstuck wrapper for sockets of type SOCK_STREAM
	    
	    This socket wrapper class is used to wrap sockets of type SOCK_STREAM
	    and provides all the functionality of those sockets but in an
	    asynchronous format. These functions are listen, connect, §accept, send,
	    recv & close.
	"""
	def listen(self, backlog):
		""" Sets the socket into listen mode.
		
		    This works in the same way as a BSD socket that is set into listen
		    mode. The underlying socket is put into listen mode, an acceptance
		    queue is created to receive the acceptance
		"""
		self.socket.listen(backlog)
		self.acceptQueue = EventQueue(self.socket.fileno(), select.EPOLLIN,
		                              self.__acceptSocket)
		self.state = STATE_LISTENING
	
	@asynchronous
	def accept(self):
		""" Accepts the next incoming socket.
		
		    When awaited, will block until a socket connects. The socket will be
		    wrapped in its own UnstuckSocketStream wrapper.
		"""
		# Add a waiting socket on the acceptance queue.
		if self.state == STATE_LISTENING:
			return self.acceptQueue.get()
		
		# Cannot accept a socket from a socket that is not listening.
		else:
			return errorFuture(Exception("UnstuckSocketStream was not"
			                             "actually listening."))
	
	@asynchronous
	def connect(self, address):
		""" Attempt to connect the socket to `address'.
		
		    This is an asynchronous version of the standard connect method. It
		    will return a Future that will be completed (with None) when the
		    connection is succesful, or will show an exception if the connection
		    failed.
		"""
		# Attempt the non-blocking connection, pass any errors into an error
		# Future.
		resultCode = self.socket.connect_ex(address)
		if resultCode != errno.EINPROGRESS:
			return errorFuture(os.strerror(resultCode))
		
		# Success: Set-up the callback to complete the connection or set the
		# error on the return Future.
		else:
			return EventFuture(self.socket.fileno(), select.EPOLLOUT,
			                   self.__connectDone, address)
	
	@asynchronous
	def send(self, buf):
		""" Send the data 'buf' down the wire.
		
		    Will queue the data to be sent as soon as the method is called and
		    return a Future. This will complete when the data has been
		    transmitted (await the future for a blocking call).
		"""
		if self.state != STATE_CONNECTED:
			return errorFuture(BrokenPipeError(
			                     "send: Socket was not connected"))
		else:
			return self.writer.write(buf)
	
	@asynchronous
	def recv(self, length):
		""" Receives 'length' bytes of data from the wire. Will return a future
		    that completes with the data when, and only when, the full amount
		    of data is available.
		"""
		if self.state != STATE_CONNECTED:
			return errorFuture(BrokenPipeError(
			                     "recv: Socket was not connected"))
		else:
			return self.reader.read(length)
	
	@asynchronous
	def close(self):
		""" Close the socket and cease communication.
		
		    Completes all ungoing data-transfers and closes the underlying
		    socket. Returns a Future that will complete when the socket is
		    succesfully closed.
		"""
		# The socket is a listening socket: Wait for pending `accepts'
		if self.state == STATE_LISTENING:
			self.state = STATE_CLOSING
			return self.__listenCloser()
		
		# The socket is a client connected socket:
		#   Wait for pending `send' and `recv'
		elif self.state == STATE_CONNECTED:
			self.state = STATE_CLOSING
			return self.__closer()
		
		# The socket is already closed
		else:
			return doneFuture
	
	def forceClose(self, error = InterruptedTransfer):
		# The socket is a listening socket:
		#   Close down pending `accepts' with an error.
		if self.state == STATE_LISTENING:
			self.state = STATE_CLOSING
			self.__rudeListenCloser(error)
		
		# The socket is a client connected socket:
		#  Close down pending operations with an error.
		elif self.state == STATE_CONNECTED:
			self.state = STATE_CLOSING
			self.__rudeCloser(error)
		
		# The socket is already closed
		else:
			pass
	
	@asynchronous
	def __listenCloser(self):
		yield from self.acceptQueue.close()
		self.__commonCloser()
		self.acceptQueue = None
	
	def __rudeListenCloser(self, error):
		for item in self.acceptQueue.forceClose():
			item.setError(error)
		self.__commonCloser()
		self.acceptQueue = None
	
	@asynchronous
	def __closer(self):
		# Run the release of ReadWrapper and WriteWrapper in parallel
		readerRelease = async(self.reader.release())
		yield from self.writer.release()
		yield from readerRelease
		self.writer = None
		self.reader = None
		self.__commonCloser()
	
	def __rudeCloser(self, error):
		# Run the release of ReadWrapper and WriteWrapper in parallel
		self.reader.forceRelease(error)
		self.writer.forceRelease(error)
		self.writer = None
		self.reader = None
		self.__commonCloser()
	
	def __commonCloser(self):
		#dispatcher.scheduleMediumPriority(self.__closeSocket)
		self.socket.close()
		self.state = STATE_CLOSED

	def __closeSocket(self):
		self.socket.close()

	def __acceptSocket(self, mask):
		try:
			accepted, address = self.socket.accept()
		# Remote side terminated the connection attempt before the local side
		# could accept it.
		except:
			raise IOEventAbort
		
		# Create the socket wrapper using the accepted low-level socket
		neonate = _USocketStream(innerSocket = accepted)
		neonate.__connectIt()
		return neonate
	
	def __connectDone(self, mask, remoteAddress):
		""" Callback used by connect to complete the connection attempt
		
		    This handle is registered when a `connect' call is made. It signals
		    that either the low-level socket is writeable (connection was
		    succesful), or there is an error condition (connection failed).
		    A success will result in a None return, failure will result in the
		    connection error being passed back through the Future.
		"""
		# Connection attempt was a failure. Retrieve the error object for the
		# Future.
		if mask & select.EPOLLERR:
			# Trying the connect again (when it is known that it will fail)
			# will cause the appropriate error to be raised.
			self.socket.connect(remoteAddress)
			# Fallback for the above
			raise(Exception("Serious failure, connect did not raise an error"))
		
		# Connection attempt was a success. Set up the wrappers and return
		# without exception.
		else:
			self.__connectIt()
	
	def __connectIt(self):
		""" Private method used to complete socket connection.
		"""
		self.state = STATE_CONNECTED
		wrapper = _SocketWrapper(self.socket)
		self.reader = ReadWrapper(wrapper)
		self.writer = WriteWrapper(wrapper)
	

class _SocketWrapper(socket.socket):
	def __init__(self, socket):
		self.inner = socket
		self.buf = []
	
	def read(self, length):
		data = self.inner.recv(length)
		if data == b"":
			raise(BrokenPipeError("Socket closed"))
		return data
	
	def write(self, buf):
		return self.inner.send(buf)
	
	def flush(self):
		pass
	
	def fileno(self):
		return self.inner.fileno()
