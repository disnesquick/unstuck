from .core import *
from .aux import *
from collections import deque

class Queue:
	""" An asynchronous Queue implementation.
	
	    Queue is a generic class used to asynchronously queue and dequeue
	    various objects. An object can be `put' onto the Queue and one of three
	    things will happen. If there is another process waiting for an object
	    then the object will be passed directly to that process. If there are
	    no processes waiting and the queue is not filled then the object will
	    be added to the internal deque. If the queue is filled then the `put'
	    will block until there is space.
	    A `get' will function in an equal but opposite way. If there are values
	    in the internal deque then one of these will be returned. If there is
	    a process then waiting to put, its value will be shifted on to the deque
	    and it will be unblocked. If the Queue is empty, the `get' will block
	    until released by a `put'.
	"""
	def __init__(self, length = -1):
		if length == -1:
			self.values = deque()
		else:
			self.values = deque(maxlen=length)
		self.getwaiters = deque()
		self.putwaiters = deque()
		self.length = length
	
	def sumSize(self):
		""" Returns the `virtual' length of the Queue.
		
		    With synchronous queues, the length of the queue will always be
		    greater than -1. However, with an asynchronous queue, the length of
		    the queue can be negative, in the case where there are processes
		    waiting on an empty Queue.
		"""
		return len(self.values) + len(self.putwaiters) - len(self.getwaiters)
	
	@asynchronous
	def put(self, value):
		""" Add an item to the Queue, blocking if it is full.
		
		    Tries to add `value' to the Queue. If possible, this will add the
		    value asynchronously (i.e. it will return a done future). If this
		    is not possible then it will block until a space in the Queue is
		    open.
		"""
		if len(self.getwaiters) > 0:
			self.getwaiters.popleft().setResult(value)
			return doneFuture
		elif self.length > len(self.values):
			self.values.append(value)
			return doneFuture
		else:
			getBarrier = Future()
			self.putwaiters.append((getBarrier, value))
			return getBarrier
	
	@asynchronous
	def get(self):
		""" Retrieve an item from the Queue, blocking if it is empty.
		
		    Tries to retrieve a single value from the Queue. If no value is
		    available then this will block until a value is available.
		"""
		fut = Future()
		if len(self.putwaiters) > 0:
			getBarrier, value = self.putwaiters.popleft()
			if self.length > 0:
				myValue = self.values.popleft()
				self.values.append(value)
			else:
				myValue = value
			getBarrier.setResult(None)
		elif len(self.values) > 0:
			value = self.values.popleft()
		else:
			self.getwaiters.append(fut)
			return fut
		fut.setResult(value)
		return fut


class XQueue(Queue):
	def putResult(self, value):
		if len(self.getwaiters) > 0:
			self.getwaiters.popleft().setResult(value)
			return doneFuture
		elif self.length > len(self.values):
			self.values.append((True, value))
			return doneFuture
		else:
			getBarrier = Future()
			self.putwaiters.append((getBarrier, (True, value)))
			return getBarrier
	
	def putError(self, error):
		if len(self.getwaiters) > 0:
			self.getwaiters.popleft().setError(error)
			return doneFuture
		elif self.length > len(self.values):
			self.values.append((False, value))
			return doneFuture
		else:
			getBarrier = Future()
			self.putwaiters.append((getBarrier, (False, value)))
			return getBarrier
	
	def get(self):
		fut = Future()
		if len(self.putwaiters) > 0:
			getBarrier, value = self.putwaiters.popleft()
			if self.length > 0:
				myValue = self.values.popleft()
				self.values.append(value)
			else:
				myValue = value
			getBarrier.setResult(None)
		elif len(self.values) > 0:
			myValue = self.values.popleft()
		else:
			self.getwaiters.append(fut)
			return fut
		
		success, value = myValue
		if success:
			fut.setResult(value)
		else:
			fut.setError(value)
		return fut


