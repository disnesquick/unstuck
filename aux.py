from .core import *
from collections import deque
from time import time
from . import core


__all__ = ["EventFuture", "Barrier", "sleep", "wrapFutureErrors", "doneFuture",
           "errorFuture", "EventQueue", "RecurringEvent"]

# This is a special Future which has been completed. Is used whenever a function
# that would return a Future for one branch, returns immediate completion on a
# separate branch.
doneFuture = Future()
doneFuture.setResult(None)

# This is a helper function to produce a Future object with an error already set
# on it. Used for situations where one branch of a function can produce an
# immediate failure.
def errorFuture(error):
	fut = Future()
	fut.setError(error)
	return fut


class IOEventAbort(Exception):
	pass


class EventFuture(Future):
	""" A Future that is hooked to an IO event.
	
	    This class is used to wait on a `select'able event from the evnt system.
	    When instantiated, an EventFuture will register itself with the event
	    system on the object and mask supplied. When the handle is called, the
	    inner handle is called. In the event of an IOEventAbort, no further
	    action is taken. Otherwise, the result/error is passed into the Future
	    behaviour and the event is de-registered.
	"""
	def __init__(self, fileNumber, mask, handle, *moreArgs):
		super().__init__()
		self.fileNumber = fileNumber
		self.mask = mask
		self.innerHandle = handle
		self.moreArgs = moreArgs
		dispatcher.registerFileEvent(fileNumber, mask, self.__handle)
	
	def __handle(self, mask):
		""" Handler called by the event system.
		
		    This handler retrieves the result/error from the inner handler,
		    which is provided with the mask and the moreArgs provided on
		    construction.
		"""
		dispatcher.unregisterFileEvent(self.fileNumber, self.mask)
		try:
			result = self.innerHandle(mask, *self.moreArgs)
			self.setResultFast(result)
		except IOEventAbort:
			core.dispatcher.registerFileEvent(fileNumber, self.mask, self.__handle)
		except Exception as error:
			self.setErrorFast(error)

#TODO finish class comment
#TODO add an optional buffer
class EventQueue:
	""" A class used to wait for objects from file events.
	
	"""
	def __init__(self, fileNumber, mask, handle, *moreArgs):
		self.queue = deque()
		self.closeBarrier = Barrier()
		self.innerHandle = handle
		self.fileNumber = fileNumber
		self.mask = mask
		self.moreArgs = moreArgs
		self.closing = False
	
	def get(self):
		""" Retrieve an object fron the EventQueue.
		
		    Adds a request to the queue for an object from the event system.
		    A Future is created and added to the right-most end of the inner
		    queue. If the queue was empty (and by implication, the FileEvent
		    is inactive, then also register the FileEvent with the event
		    system. If the EventQueue is closing then set an error on the
		    returned future and do not add it to the queue.
		"""
		fut = Future()
		if self.closing:
			fut.setError(Exception("EventQueue is closed"))
		else:
			self.queue.append(fut)
			if len(self.queue) == 1:
				dispatcher.registerFileEvent(self.fileNumber, self.mask,
				                             self.__handle)
		return fut
	
	def close(self):
		""" Closes the EventQueue gracefully.
		
		    This is a coroutineish that induces a graceful close on the
		    queue. The queue will no longer accept `get' calls (and will
		    raise an exception if any are attempted. The closeBarrier will be
		    returned. This is released when all the waiting Futures on the
		    queue have been fulfilled by the event system.
		"""
		# Already closing, just return the closebarrier
		if self.closing:
			return self.closeBarrie
		self.closing = True
		
		# Nothing waiting, so release the closeBarrier already, the EventQueue
		# is already closed, thereafter.
		if len(self.queue) == 0:
			self.closeBarrier.release()
		
		return self.closeBarrier
	
	def forceClose(self):
		""" Force the EventQueue to close prematurely.
		
		    This is a generator which should be used with caution. It is used
		    when something has gone very wrong and the EventQueue must be
		    cleared without waiting for the Futures therein to be fulfilled by
		    the event system. Will yield all the waiting Futures so that the
		    error condition (usually) or result can be set by the caller.
		"""
		if len(self.queue) > 0:
			dispatcher.unregisterFileEvent(self.fileNumber, self.mask)
		self.closing = True
		self.closeBarrier.release()
		while len(self.queue) > 0:
			yield self.queue.popleft()
	
	def __handle(self, mask):
		""" Process an object from the underlying file and pass it to the queue.
		
		    This function is the handle registered with the event system. It
		    calls the inner handle, which functions to retrieve a single object
		    from the underlying file object. This function only supplies the
		    mask to the registered handle, as it receives for itself. In
		    contrast to an event handle, the inner handle is expected to return
		    a result or error. This result/error is passed to the left-most
		    Future on the queue. The inner handle could also raise IOEventAbort,
		    which terminates this function as no object was available in spite
		    of what the event system thinks.
		"""
		# The innerHandle is used to retrieve the result for the queue.
		fut = self.queue.popleft()
		try:
			getThing = self.innerHandle(mask, *self.moreArgs)
			fut.setResult(getThing)
		# IOEventAbort is raised by an innerHandle to signify that a result
		# was not available. The future is not removed from the queue here.
		except IOEventAbort:
			self.queue.pushleft(fut)
			return
		# Any other exception should be raised on the Future from the queue.
		except Exception as e:
			fut.setError(e)
		
		# If the EventQueue is empty then make sure to unregister the handle
		# so that the event system is not trying to accept connections that are
		# not being waited for.
		if len(self.queue) == 0:
			# in closing mode. This is the last thing that is being waited for
			# so fulfill the closing barrier.
			if self.closing:
				self.closeBarrier.release()
			dispatcher.unregisterFileEvent(self.fileNumber, self.mask)


def sleep(forTime):
	""" Suspend process for at least the number of seconds provided.
	
	    Coroutine sleep does not guarantee that execution will be resumed at
	    exactly the time supplied but guarantees that execution will be
	    suspended for at least that amount of time.
	"""
	fut = Future()
	def inner():
		fut.setResult(None)
	core.dispatcher.scheduleHandleByTime(time() + forTime, inner)
	return fut


class _ControlYield(Future):
	def setCallback(self, cb):
		core.dispatcher.scheduleLowPriority(lambda: cb.send(None))
	
	def setResult(self, result):
		raise(Exception("ControlYield cannot have a result"))
	
	def setError(self, error):
		raise(Exception("ControlYield cannot have an error"))
	
	def apply(self, gen):
		raise(Exception("How did you even get to this part of the code?"))
	
	def getResult(self):
		raise(Exception("Cannot retrieve a result from ControlYield"))


controlYield = _ControlYield()


class RoundRobin:
	""" A handy little gate which allows control to be passed between several
	    co-routines
	"""
	def __init__(self, count = 2):
		self.waiting = deque()
		self.count = count
	
	def swap(self, value = None):
		myWaiter = Future()
		self.waiting.append(myWaiter)
		if len(self.waiting) == self.count:
			self.waiting.popleft().setResult(value)
		return myWaiter
	
	def done(self, value = None):
		self.count -= 1
		if self.count > 0 and len(self.waiting) == self.count:
			self.waiting.popleft().setResult(value)


class FirstPastThePost(Future):
	def setResult(self, result):
		if not self.isDone:
			super().setResult(result)
	
	def setError(self, error):
		if not self.isDone:
			super().setError(result)


class ErrorGate(Future):
	""" Error gate allows errors to be injected past another future into a
	    waiting process.
	"""
	def __init__(self, clearSchedule=20):
		super().__init__()
		self.gatedFutures = []
		self.clearSchedule = clearSchedule
		self.hookCount = 0
	
	def setResult(self):
		raise(Exception("Cannot set a result on an ErrorGate"))
	
	def setError(self, error):
		self.done()
		self.error = error
		for fut in self.gatedFutures:
			fut.setError(error)
	
	def __call__(self, thing):
		if self.isDone:
			return self
		
		self.hookCount += 1
		
		if isinstance(thing, Future):
			fut = thing
		else:
			fut = async(thing)
		
		if fut.isDone:
			return fut
		
		# Periodically clear out the list of gated futures
		if self.hookCount >= self.clearSchedule:
			self.hookCount = 0
			newList = []
			for gfut in self.gatedFutures:
				if not gfut.isDone:
					newList.append(gfut)
			self.gatedFutures = newList
		
		pfut = FirstPastThePost()
		fut.setCallback(pfut)
		
		self.gatedFutures.append(pfut)
		
		return pfut


class Barrier(Future):
	""" A parameter-free Future with multiple waiters.
	
	    Barrier provides, as expected, a barrier to execution. A Barrier is
	    created when other procedures need to be able to wait for a procedure
	    to finish in another process. The primary differences between a Barrier
	    and a Future are that a Barrier is valid for use by multiple processes
	    and a Barrier does not provide any kind of return: A process can only
	    wait until another process releases the Barrier; no error or value
	    can be sent through it.
	"""
	def getResult(self):
		pass
	
	def release(self):
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			for cb in self.cb:
				core.dispatcher.scheduleHighPriority(cb.send, None)
	
	def releaseFast(self):
		assert not self.isDone
		self.isDone = True
		if self.cb is not None:
			for cb in self.cb:
				cb.send(None)
	
	def setCallback(self, cb):
		if self.cb is None:
			self.cb = [cb]
		else:
			self.cb.append(cb)

class _Wrapper:
	def __init__(self, cb):
		self.cb = cb
	
	def send(self, result):
		pass
	
	def throw(self, error):
		self.cb(error)


def wrapFutureErrors(callback, *args):
	wrapper = _Wrapper(callback)
	for fut in args:
		if fut.isDone and fut.error is not None:
			callback(fut.error)
		else:
			fut.setCallback(wrapper)


class RecurringEvent:
	""" A convenience class for scheduling repeating events.
	
	    This class is used for scheduling an event that will repeat at a regular
	    time interval. Once begun, an event will keep repeating at its set
	    interval until `stop' is called; after this is called, the recurring
	    event will be immediately stopped: No further callbacks will be acted
	    on, even though one will be in the schedulers pipeline.
	"""
	def __init__(self, interval, callback, *args):
		self.callback = callback
		self.args = args
		self.interval = interval
		self.running = False
		self.stopping = False
	
	def __call__(self):
		self.callback(*self.args)
		if self.stopping:
			self.running = False
			self.stopping = False
		else:
			self.__schedule()
	
	def begin(self):
		""" Start the regular callbacks running.
		
		    This function sets the regular callbacks running. If the object
		    was in the process of stopping, then this process will be halted and
		    the previously scheduled callback will be run at the time it was
		    scheduled for. If the object was otherwise not running, then the
		    callback will be scheduled for `self.interval' seconds in the
		    future. If the object was already running, an error is raised.
		"""
		if self.stopping:
			self.stopping = False
		elif self.running:
			raise(Exception("RecurringEvent was already running"))
		else:
			self.running = True
			self.__schedule()
		return self
	
	def stop(self):
		""" Stop the regular callbacks from running.
		
		    This function stops the regular events running immediately. It is
		    likely that a callback will be in the dispatcher pipeline but this
		    is marked not to run by setting the current state to `stopping'. If
		    the object was not running, an error is raised.
		"""
		if not self.running:
			raise(Exception("RecurringEvent was not running"))
		self.stopping = True
	
	def __schedule(self):
		""" Private function to handle scheduling.
		
		    This function schedules this object's callback to run at the set
		    time-point in the future.
		"""
		dispatcher.scheduleHandleByTime(time()+self.interval, self)
