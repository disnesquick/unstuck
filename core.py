from inspect import isgeneratorfunction
from collections import deque
from time import time
from greenlet import greenlet

from .events import Dispatcher

__all__ = ["dispatcher", "Future", "async", "await", "asynchronous", "callSoon",
           "callLater", "callAt", "flushEvents","forkDispatcher", "awaitAll"]

dispatcher = Dispatcher()

flushEvents = dispatcher.flush
forkDispatcher = dispatcher.handleFork
callSoon = dispatcher.scheduleHighPriority
callLater = dispatcher.scheduleMediumPriority
callAt = dispatcher.scheduleHandleByTime


def asynchronous(inner):
	""" This is a simple decorator, used to mark asynchronous functions, i.e.
	    those that are generators and will yield futures, those that will
	    return instantiated asynchronous generators or those that will return
	    a Future.
	"""
	return inner


class Future:
	""" An object representing a delayed result or error.
	    This is the fundamental unit of asynchronous control. It functions in
	    one of two ways. Either the result can be set before any other fibre
	    waits on the future, in which case, the result (or error) is stored for
	    latter retrieval. In the second instance, another fibre waits on the
	    Future before a result is set. In this case, a callback is scheduled so
	    that the waiting fibre will be woken when the result is set later.

	    To maintain compatibility with generators, a Future can be "yielded
	    from" but, in fact, this is just a wrapper for the fuure itself and
	    will be slower than a basic "yield". A basic yield on something other
	    than a future will cause problems, though, so "yield from" is preferred
	    in cases of uncertainty.
	"""
	def __init__(self):
		self.isDone = False
		self.cb = None
		self.error = None

	def __del__(self):
		""" Garbage collection.
	
		    When a Future is garbage collected, and it has an error condition
		    but this error condition was never accessed, then an error should be
		    raised, informing the application that something failed silently.
		"""
		if self.error is not None and not self.errorAccessed:
			raise self.error
			raise Exception("Error failed silently at "
			        "garbage collection [%s]"%self.error) from self.error
	
	def getResult(self):
		""" Returns the result or raises the error.
		
		    This is called on a Future that has completed. It will not check
		    whether the future has actually completed, since it assumes that
		    that check has already been carried out. If the Future was succesful
		    then the result is returned. If it failed then the error is raised.
		"""
		if self.error is not None:
			self.errorAccessed = True
			raise(self.error)
		else:
			return self.result
	
	def setResult(self, result):
		""" Succesful completion of a Future with a return value.
		
		    This function informs a Future of succesful completion and supplies
		    the return value. The following behaviour depends on the callback.
		    If another process is already waiting on the Future then a callback
		    will have been set. This will be scheduled by the calling function.
		    Otherwise the result will be left for retrieval by a future process.
		"""
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			dispatcher.scheduleHighPriority(self.cb.send, result)
		else:
			self.result = result
	
	def setResultLate(self, result):
		""" As setResult but with delayed callback behavioue
		
		    The interface is identical to setResult but whereas that function
		    will schedule the callback at the front of the execution queue, this
		    will schedule the callback at the end of the queue.
		"""
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			dispatcher.scheduleMediumPriority(self.cb.send, result)
		else:
			self.result = result
	
	def setResultFast(self, result):
		""" As setResult but with immediate callback behaviour.
		
		    The interface is identical to setResult but whereas that function
		    will schedule the callback, this method will immediately call the
		    callback from the current process. Should be used with caution and
		    is intended to be used at the end of IO handlers as a form of tail
		    call optimization.
		"""
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			self.cb.send(result)
		else:
			self.result = result
	
	def setError(self, error):
		""" Signal failure of a Future with an error object.
		
		    This function informs a Future of failure and supplies the error
		    object. The subsequent behaviour depends on the callback.
		    If another process is already waiting on the Future then a callback
		    will have been set. This will be scheduled by the calling function.
		    Otherwise the error will be left for retrieval by a future process.
		"""
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			# Schedule the helper function so that the silent-error mitigation
			# can work properly.
			dispatcher.scheduleHighPriority(self.__cbError, error)
		else:
			self.errorAccessed = False
			self.error = error
	
	def setErrorLate(self, error):
		""" As setError but with delayed callback behavioue
		
		    The interface is identical to setError but whereas that function
		    will schedule the callback at the front of the execution queue, this
		    will schedule the callback at the end of the queue.
		"""
		
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			# Schedule the helper function so that the silent-error mitigation
			# can work properly.
			dispatcher.scheduleMediumPriority(self.__cbError, error)
		else:
			self.errorAccessed = False
			self.error = error
	
	def __cbError(self, error):
		""" Helper function for scheduling failure.
		
		    This function is used by setError to schedule a callback with error
		    condition. It ensures that the book-keeping is updated so that the
		    garbage collector will know that this error was accessed.
		"""
		self.errorAccessed = True
		self.cb.throw(error)
	
	def setErrorFast(self, error):
		""" As setError but with immediate callback behaviour.
		
		    The interface is identical to setError but whereas that function
		    will schedule the callback, this method will immediately call the
		    callback from the current process. Should be used with caution and
		    is intended to be used at the end of IO handlers as a form of tail
		    call optimization.
		"""
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			self.errorAccessed = True
			self.cb.throw(error)
		else:
			self.errorAccessed = False
			self.error = error
	
	def setCallback(self, cb):
		""" Set the callback function.
		
		    This function is used when an unfulfilled Future is encounted in a
		    yield loop to allow that loop to be awoken when this Future is
		    fulfilled.
		"""
		self.cb = cb
	
	def __iter__(self):
		""" The function that 'yield from' hooks into
		
		    This function will return the Future result if it is done, otherwise
		    it will pass itself back to the root Task to have its callback set.
		"""
		if not self.isDone:
			return (yield self)
		else:
			return self.getResult()
	
	__await__ = __iter__


def await(thing):
	if not isinstance(thing, Future):
		thing = _Task(thing)
		thing.send(None)
	if thing.isDone:
		return thing.getResult()
	else:
		return _handleEvents(thing)


def awaitAll(*things):
	return tuple(await(_awaitAllInner(things)))

def _awaitAllInner(things):
	return [(yield from thing) for thing in things]


def async(gen):
	""" Launch a generator as an asynchronous task.
	
	    This function is used to spawn a coroutine parallel to the current
	    process. A coroutine, when created will not run until yielded or
	    asynced. Async means that all the IO-callbacks will be created so
	    that this coroutine can completed whilst other tasks are being awaited.
	"""
	# Return a Future immediately, async shouldn't be used this way.
	if isinstance(gen, Future):
		return gen
	
	# Wrap the coroutine in an _AsyncTask and start it off. It will behave like
	# a Future from the point-of-view of the async caller.
	else:
		task = _Task(gen)
		dispatcher.scheduleHighPriority(task.send, None)
		return task


class _Task(Future):
	""" Task wrapper used by async.
	
	    This class wraps a coroutine and functions like a Future. The coroutine
	    will be fed Future results, in a similar manner to the await command but
	    this class will not defer to the eventLoop, as that must be handled by
	    an await command.
	    This class will also before like a generator itself so that it can be
	    the target of a callback.
	"""
	def __init__(self, gen):
		super().__init__()
		self.gen = gen
	
	def send(self, result):
		""" Receive a return value for the most recent yield.
		
		    This method functions in the same way as a generator 'send'
		    when a new value is received, it is sent to the inner coroutine and
		    the future loop is entered. A non-fulfilled Future is received, then
		    the callback is scheduled on this object. When the inner coroutine
		    completes, the result/error is set on this object for Future mode.
		"""
		try:
			self.gen.send(result).setCallback(self)
		
		# Succesful completion of inner coroutine
		except StopIteration as e:
			self.setResultFast(e.value)
		
		# Failure of inner coroutine
		except Exception as e:
			self.setErrorFast(e)
		
	def throw(self, error):
		""" Receive an exception for the most recent yield.
		
		    see 'send' above.
		"""
		try:
			self.gen.throw(error).setCallback(self)
		
		# Succesful completion of inner coroutine
		except StopIteration as e:
			self.setResultFast(e.value)
		
		# Failure of inner coroutine
		except Exception as e:
			self.setErrorFast(e)


class _GreenTask:
	""" Wrapper class to enable a greenlet to behave like a coroutine.
	
	    This simple wrapper class initializes itself with the currently running
	    greenlet (this is the only greenlet of interest for this library) and
	    exposes the same interface as the python coroutines do. When a send or a
	    throw is received, it is translated into a call to the _eventLoop
	    greenlet component, signalling the origin of the switch and the success
	    or failure of the switch, plus the component result or error object.
	"""
	def __init__(self):
		self.green = greenlet.getcurrent()
	
	def send(self, result):
		self.green.switch(greenlet.getcurrent(), result, True)
	
	def throw(self, error):
		self.green.switch(greenlet.getcurrent(), error, False)

_rootGreenlet = None
def _handleEvents(fut):
	""" This function is responsible for distributing switches to the event
	    loop.
	
	    The event loop is 'claimed' by the first await call that reaches this
	    function and is then responsible for handling subsequent await calls.
	    The theory is that only the event loop can cause truly parallel calls
	    to be made, such that an await from an event loop callback may not be
	    related to the master await. It must therefore be put into its own
	    greenlet. This is handled by eventLoop. This prevents one await being
	    dependent on another, unrelated await.
	
	    This function will return as soon as the initial await is satisfied and
	    the function that caused it to be satisfied has been completed.
	"""
	global _rootGreenlet
	thisTask = _GreenTask()
	fut.setCallback(thisTask)
	
	#TODO What if the loop master is not the root greenlet?
	# There is currently no loop master, create the loop master and wait for new
	# sub-greenlets, as well as the result/error on the loop master greenlet.
	if _rootGreenlet is None:
		_rootGreenlet, poll = thisTask.green, None
		while poll is None:
			poll = _eventLoop()
		origin, ret, success = poll
		_rootGreenlet = None
		origin.switch()
	
	# There is already a loop master so tell it to start a new event processor
	# for this process
	else:
		_, ret, success = _rootGreenlet.switch(None)
	
	if success:
		return ret
	else:
		raise ret


def _eventLoop():
	""" Used by _handleEvents to create a new event-polling greenlet.
	
	    This function is called from the root greenlet to create a new event
	    task. Control will be passed to a child greenlet, which will retrieve
	    events from the event dispatcher until one of two things happens. Either
	    another _handleEvents call will be made, which will result in the
	    current greenlet finishing its last handle, or the current greenlet has
	    its future fulfilled, which will result in exit from the outside loop
	    (in _handleEvents) too.
	"""
	running = True
	def inner():
		while running:
			dispatcher.runNextHandle()
	ret = greenlet(inner).switch()
	running = False
	return ret
