from collections import deque
import select
import fcntl
import os
import struct

from .core import *
from .aux import *
from .events import errorCheckingMask

class InterruptedTransfer(Exception):
	pass

class StreamClosed(Exception):
	pass


def setNonblocking(fileDesc):
	""" sets a file descriptor to be non-blocking in Linux
	"""
	fl = fcntl.fcntl(fileDesc, fcntl.F_GETFL)
	fcntl.fcntl(fileDesc, fcntl.F_SETFL, fl | os.O_NONBLOCK)


class ReadWrapper:
	""" This class is used to handle the reading from a file object.
	
	    It works by putting a buffer between the operating system file handle
	    and the asynchronous code. This creates the seperation between readInto
	    and readFrom where readFrom is the application layer reading from this
	    buffer and readInto is the operating system putting data into the
	    buffer. All reads from the buffer will result in a future being
	    returned, which can be awaited (explicitly or through yield) to allow
	    blocking to be delayed.
	"""
	def __init__(self, fileObject, lowBuffer = 128, highBuffer = 256):
		setNonblocking(fileObject)
		self.bufSize = 0
		self.bufSizeLow = lowBuffer
		self.bufSizeHigh = highBuffer
		self.buf = []
		self.readWaiters = deque()
		self.fileNumber = fileObject.fileno()
		self.iread = fileObject.read
		self.readWaitingSize = 0
		self.readClosing = None
		if self.bufSizeHigh > 0:
			self.__registerReader()
		else:
			self.registeredReader = False
	
	def __del__(self):
		if self.readClosing is None or not self.readClosing.isDone:
			self.forceRelease()
	
	def read(self, length):
		""" Works in the same way as a normal file object read method except
		    that it returns a future, rather than the amount read. 'awaiting'
		    the future will block until the entire read is finished.
		"""
		fut = Future()
		if self.readClosing is not None:
			# Cannot read from a closing wrapper
			fut.setError(Exception("Read on released wrapper"))
		elif len(self.readWaiters) == 0 and self.bufSize >= length:
			sumLength = 0
			sumBit = []
			remainder = []
			while sumLength < length:
				nextBit = self.buf[len(sumBit)]
				if sumLength + len(nextBit) > length:
					remnant = length - sumLength
					sumBit.append(nextBit[:remnant])
					remainder = [nextBit[remnant:]]
					sumLength += remnant
				else:
					sumLength += len(nextBit)
					sumBit.append(nextBit)
			
			self.buf = remainder + self.buf[len(sumBit):]
			fut.setResult(b"".join(sumBit))
			self.bufSize -= length
			if not self.registeredReader and self.bufSize < self.bufSizeLow:
				self.__registerReader()
		else:
			if not self.registeredReader:
				self.__registerReader()
			self.readWaitingSize += length
			self.readWaiters.append((fut, length))
		
		return fut
	
	def readline(self):
		""" Reads a <LF> terminated block from the stream.
		
		    This method returns a Future that will be fulfilled with a data
		    block running from the current position to the first line-feed
		    character encountered. The line-feed will be included in the
		    returne blob of data.
		"""
		if self.readClosing is not None:
			return errorFuture(InterruptedTransfer("Read on released wrapper"))
		
		fut = Future()
		if len(self.readWaiters) == 0 and self.bufSize > 0:
			if len(self.buf) > 1:
				bufString = b"".join(self.buf)
			else:
				bufString = self.buf[0]
			bufSplit = _smartSplit(bufString)
			if bufSplit is not None:
				self.buf = [bufSplit[1]]
				self.bufSize = len(bufSplit[1])
				fut.setResult(bufSplit[0])
				return fut
			else:
				self.buf = [bufString]
		
		self.readWaiters.append((fut, -1))
		if not self.registeredReader:
			self.__registerReader()
		return fut
	
	@asynchronous
	def release(self):
		""" Releases control of the underlying file object. Will wait until all
		    current reads are complete and then fulfill a future with the total
		    length of data remaining in the buffer.
		"""
		if self.readClosing is not None:
			return self.readClosing
		self.readClosing = Barrier()
		if len(self.readWaiters) == 0:
			self.__completeRelease()
		return self.readClosing
	
	def forceRelease(self, error = InterruptedTransfer):
		""" A rather ruder version of release.
		    
		    Forces the stream to close down, interrupting all waiting reads 
		    with an InterruptedTransfer exception.
		"""
		if self.readClosing is None:
			self.readClosing = Barrier()
		while len(self.readWaiters) > 0:
			fut, length = self.readWaiters.popleft()
			fut.setError(error)
		self.__completeRelease()
	
	@asynchronous
	def readPacket1(self):
		""" Read a size-tagged packet from a stream.
		
		    Reads a discrete packet of data from the stream. The end of the
		    packet is identified by a 1-byte length header.
		"""
		length = yield from self.read(1)
		length, = struct.unpack(">B",length)
		packet = yield from self.read(length)
		return packet
	
	@asynchronous
	def readPacket2(self):
		""" Read a size-tagged packet from a stream.
		
		    Reads a discrete packet of data from the stream. The end of the
		    packet is identified by a 2-byte length header.
		"""
		length = yield from self.read(2)
		length, = struct.unpack(">H",length)
		packet = yield from self.read(length)
		return packet
	
	@asynchronous
	def readPacket4(self):
		""" Read a size-tagged packet from a stream.
		
		    Reads a discrete packet of data from the stream. The end of the
		    packet is identified by a 4-byte length header.
		"""
		length = yield from self.read(4)
		length, = struct.unpack(">I",length)
		packet = yield from self.read(length)
		return packet
	
	def __handleReadInto(self, mask):
		""" This is the callback registered with the event loop whenever there
		    is a future waiting on data to be read. Will only read as much data
		    as total requested data from all the futures waiting for data and
		    then fulfill as much as possible.
		"""
		try:
			if mask & errorCheckingMask:
				if mask & select.EPOLLERR:
					raise(Exception("Error on file object"))
				else:
					raise(StreamClosed("Stream closed"))
			if len(self.readWaiters) > 0 and self.readWaiters[0][1] == -1:
				data = self.iread(self.bufSizeHigh)
			else:
				data = self.iread(self.readWaitingSize + self.bufSizeHigh
					                     - self.bufSize)
			data = self.__fillWaiters(data)
			# Add remaining data into the buffer.
			self.buf.append(data)
			self.bufSize += len(data)
		
		except Exception as e:
			if len(self.readWaiters) > 0:
				self.readWaiters.popleft()[0].setError(e)
			
			# Horrible: This code is repeated from below with one small
			# modification.
			if self.registeredReader and len(self.readWaiters) == 0:
				if self.readClosing is not None:
					self.__completeRelease()
				else:
					self.__unregisterReader()
			return
		
		if self.registeredReader and len(self.readWaiters) == 0:
			# If the wrapper is being released and all current waiting reads
			# have finished then the future can be unblocked and the handle
			# removed from the events.
			if self.readClosing is not None:
				self.__completeRelease()
			
			# If the buffer is full then temporarily removed the handle
			# from the events.
			elif self.bufSize >= self.bufSizeHigh:
				self.__unregisterReader()
	
	def __fillWaiters(self, data):
		""" Private method to supply data to the waiting reads.
		
		    This methods takes the explicit data provided by the last inner read
		    and the data currently present in the buffer and streams it into the
		    waiting reads, in sequential order. As soon as there is sufficient
		    data for a waiting read, the Future is fulfilled with a complete
		    binary blob and that Future is removed from the front of the queue
		    to allow the next Future to be serviced. Futures are serviced until
		    there is not enough data to fulfill the Future at the fron of the
		    queue.
		"""
		if len(self.readWaiters) == 0:
			return data
		fut, nextLength = self.readWaiters[0]
		
		# The first block of reads are using a partially filled buffer
		# and the current new piece of data
		
		# Read is part of a readline so try and read up to a carriage
		# return, if possible. If not carriage return then buffer the
		# remainder.
		if nextLength == -1:
			dataSplit = _smartSplit(data)
			if dataSplit is not None:
				result, data = dataSplit
			else:
				return data
			
		# Read is part if a read-by-length and there is enough data to
		# fulfill it so chop that much data out for the waiter and leave
		# the rest as the next remainder.
		elif nextLength <= self.bufSize + len(data):
			remnant = nextLength - self.bufSize
			self.readWaitingSize -= nextLength
			result, data = data[:remnant], data[remnant:]
		else:
			return data
		
		self.buf.append(result)
		result = b"".join(self.buf)
		fut.setResult(result)
		self.readWaiters.popleft()
		self.buf = []
		self.bufSize = 0
		
		# The second block of reads differ, because by this point, the prior
		# buffer has been amalgamated into a single binary blob in the `data'
		# variable.
		while len(self.readWaiters) > 0:
			fut, nextLength = self.readWaiters[0]
			# See comment above
			if nextLength == -1:
				dataSplit = _smartSplit(data)
				if dataSplit is not None:
					result, data = dataSplit[0], dataSplit[1]
				else:
					break
			
			# See comment above
			elif nextLength <= len(data):
				result, data = data[:nextLength], data[nextLength:]
				self.readWaitingSize -= nextLength
			
			# See comment above
			else:
				break
			
			self.readWaiters.popleft()
			fut.setResult(result)
			
		
		return data
	
	def __completeRelease(self):
		if self.registeredReader:
			self.__unregisterReader()
		self.readClosing.release()
	
	def __registerReader(self):
		""" Register this reader with the event dispatcher
		"""
		self.registeredReader = True
		dispatcher.registerFileEvent(self.fileNumber, select.EPOLLIN,
		                             self.__handleReadInto)
	
	def __unregisterReader(self):
		""" Unregister this reader with the event dispatcher
		"""
		self.registeredReader = False
		dispatcher.unregisterFileEvent(self.fileNumber, select.EPOLLIN)
 

class WriteWrapper:
	""" This class is used to handle the writing to a file object.
	
	    Whereas the ReadWrapper uses a buffer between the file object and the
	    application, the write buffer doesn't need to use a buffer because the
	    object to write will already have been allocated.  All writes will
	    result in a future being returned, which can be awaited (explicitly or
	    through a yield) to allow blocking to be delayed. As soon as the write
	    is called, the data will be scheduled to be sent, however. 
	"""
	def __init__(self, fileObject):
		setNonblocking(fileObject)
		self.writeWaiters = deque()
		self.fileObject = fileObject
		self.writeWaitingSize = 0
		self.writeClosing = None
	
	def __del__(self):
		if self.writeClosing is None or not self.writeClosing.isDone:
			self.forceRelease()
	
	def write(self, buf):
		""" Works in the same way as a normal file object write method except
		    that it returns a future, rather than the amount written. 'awaiting'
		    the future will block until the entire write is finished.
		"""
		fut = Future()
		length = len(buf)
		if self.writeClosing is not None:
			fut.setError(InterruptedTransfer("Write on released wrapper"))
		elif length == 0:
			fut.setResult(0)
		else:
			if self.writeWaitingSize == 0 and length > 0:
				self.__registerWriter()
			self.writeWaitingSize += length
			self.writeWaiters.append((fut, buf, len(buf)))
		return fut
	
	def release(self):
		""" Releases control of the underlying file object.
		    
		    Will wait until all current writes are complete and then fulfill a 
		    future.
		"""
		if self.writeClosing is not None:
			return self.writeClosing
		self.writeClosing = Barrier()
		if len(self.writeWaiters) == 0:
			self.writeClosing.release()
		return self.writeClosing
	
	def forceRelease(self, error = InterruptedTransfer):
		""" A rather ruder version of release.
		    
		    Forces the stream to close down, interrupting all waiting writes
		    with an InterruptedTransfer exception.
		"""
		if self.writeClosing is None:
			self.writeClosing = Barrier()
		if len(self.writeWaiters) > 0:
			self.writeWaitingSize = 0
			self.__unregisterWriter()
			while len(self.writeWaiters) > 0:
				self.writeWaiters.popleft()[0].setError(error)
		self.writeClosing.release()
	
	@asynchronous
	def writePacket1(self, packet):
		""" Write a size-tagged packet to a stream.
		
		    Writes a discrete packet of data to the stream. The end of the
		    packet is identified by a 1-byte length header.
		"""
		length = struct.pack(">B",len(packet))
		return self.write(length+packet)
	
	@asynchronous
	def writePacket2(self, packet):
		""" Write a size-tagged packet to a stream.
		
		    Writes a discrete packet of data to the stream. The end of the
		    packet is identified by a 2-byte length header.
		"""
		length = struct.pack(">H",len(packet))
		return self.write(length+packet)
	
	@asynchronous
	def writePacket4(self, packet):
		""" Write a size-tagged packet to a stream.
		
		    Writes a discrete packet of data to the stream. The end of the
		    packet is identified by a 4-byte length header.
		"""
		length = struct.pack(">I",len(packet))
		return self.write(length+packet)
	
	def __handleWriteFrom(self, mask):
		""" Handle called when data is available from the file.
		    
		    This is the callback registered with the event loop whenever there
		    is a future waiting to write data. Will write as much data as
		    possible and then flush the stream at the end of all the writes.
		"""
		try:
			doneIndex = 0
			oldWriteWaitingSize = self.writeWaitingSize
			if mask & errorCheckingMask:
				if mask & select.EPOLLERR:
					raise(Exception("Error on file object"))
				else:
					raise(StreamClosed("Stream closed"))
			while self.writeWaitingSize > 0:
				fut, writeBytes, totalLength = self.writeWaiters[doneIndex]
				dataSize = self.fileObject.write(writeBytes)
				self.writeWaitingSize -= dataSize
				if dataSize < len(writeBytes):
					self.writeWaiters[doneIndex] = (fut, writeBytes[dataSize:],
					                                totalLength)
					break
				else:
					doneIndex += 1
			
			# Update the Future's that are waiting with their written length to
			# signal that the write has finished. This is done after the flush
			# because the failure may occur during the flush, because of
			# buffering.
			self.fileObject.flush()
			while doneIndex > 0:
				fut, _, length = self.writeWaiters.popleft()
				fut.setResult(length)
				doneIndex -= 1
		
		# Failure means that all the 'completed' write waiters, as well as the
		# most recent pending one, will be failed. This takes advantage of the
		# buffering so files are not flushed with multiple small writes if it
		# can be avoided.
		except Exception as e:
			if len(self.writeWaiters) > 0:
				_, data, _ = self.writeWaiters[0]
				self.writeWaitingSize -= len(data)
				while doneIndex >= 0:
					self.writeWaiters.popleft()[0].setError(e)
					doneIndex -= 1
		
		# oldWriteWaitingSize is used as a check in case the wrapper has been
		# released already but this is a queued handle.
		if self.writeWaitingSize == 0 and oldWriteWaitingSize > 0:
			self.__unregisterWriter()
			if self.writeClosing is not None:
				self.writeClosing.releaseFast()
	
	def __registerWriter(self):
		""" Register this writer with the event dispatcher
		"""
		dispatcher.registerFileEvent(self.fileObject.fileno(),
		                             select.EPOLLOUT, self.__handleWriteFrom)
	
	def __unregisterWriter(self):
		""" Unregister this writer with the event dispatcher
		"""
		dispatcher.unregisterFileEvent(self.fileObject.fileno(),
		                               select.EPOLLOUT)


def _smartSplit(byteObj):
	""" Split a byte string by newline without dropping the newline.
	
	    This functions is used for the readline components of the ReadWrapper.
	"""
	idx = byteObj.find(b"\n")
	if idx == -1:
		return None
	else:
		idx += 1
		return byteObj[:idx], byteObj[idx:]
