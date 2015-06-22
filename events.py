import select
from collections import deque
from time import time, mktime
import heapq

errorCheckingMask = select.EPOLLERR | select.EPOLLHUP

class HeapKeyValue:
	def __init__(self, key, value):
		self.key = key
		self.value = value
	
	def __gt__(self, other):
		return self.key > other.key
	
	def __gte__(self, other):
		return self.key >= other.key
	
	def __lt__(self, other):
		return self.key < other.key
	
	def __lte__(self, other):
		return self.key <= other.key
	
	def __eq__(self, other):
		return self.key == other.key
	
	def __ne__(self, other):
		return self.key != other.key


inf = float("inf")
class Schedule:
	def __init__(self):
		self.heap = []
	
	def scheduleHandleByTime(self, when, what, *args):
		heapq.heappush(self.heap, HeapKeyValue(when, (what, args)))
	
	def scheduleHandleByDatetime(self, when, what, *args):
		when = mktime(when.timetuple()) + when.microsecond / 1e6
		return scheduleHandleByTime(when, what, *args)
	
	def readyTime(self):
		if len(self.heap) == 0:
			return inf
		now = time()
		return self.heap[0].key - now
	
	def popHandle(self):
		return heapq.heappop(self.heap).value


class Dispatcher(Schedule):
	""" Central event-management, scheduling, and deferred callback mechanism.
	    
	    Central object which, initialized only once as a global, is responsible
	    for event polling, time-based scheduling, and giving up control of
	    execution to another process (through the lowPriority scheduling).
	"""
	def __init__(self):
		super().__init__()
		self.handles = {}
		self.handleQueue = deque()
		self.lowPriorityHandleQueue = deque()
		self.handleFork()
	
	def handleFork(self):
		self.pollingObject = select.epoll()
	
	def __del__(self):
		self.flush()
		if len(self.handles) > 0:
			raise(Exception("Dispatcher %s died with handles"
			                " still active [%s]"%(self,self.handles)))
	
	def scheduleLowPriority(self, handle, *args):
		self.lowPriorityHandleQueue.append((handle, args))
	
	def scheduleMediumPriority(self, handle, *args):
		self.handleQueue.append((handle, args))
	
	def scheduleHighPriority(self, handle, *args):
		self.handleQueue.appendleft((handle, args))
	
	def registerFileEvent(self, fd, mask, handle):
		try:
			handleList = self.handles[fd]
			# Check whether the current mask overlaps with extant handles
			# and also build the current bitmask.
			registerMask = mask
			for om in handleList:
				if om & mask:
					raise(Exception("Mask clash"))
				registerMask |= om
			self.pollingObject.modify(fd, registerMask)
			handleList[mask] = handle
		
		# Create the handle list if the current fd is not there.
		except KeyError:
			handleList = self.handles[fd] = {mask:handle}
			self.pollingObject.register(fd, mask)
	
	def unregisterFileEvent(self, fd, mask):
		try:
			handleList = self.handles[fd]
			del handleList[mask]
		except KeyError:
			raise(Exception("%d,%d was not registered" % (fd,mask)))
		
		if len(handleList) == 0:
			self.pollingObject.unregister(fd)
			del self.handles[fd]
		else:
			registerMask = 0
			for om in handleList:
				registerMask |= om
			self.pollingObject.modify(fd, registerMask)
	
	def flush(self):
		while len(self.handleQueue) > 0:
			self.runNextHandle()
	
	def runNextHandle(self):
		if len(self.handleQueue) == 0:
			timeToNext = self.readyTime()
			if timeToNext < 0.0:
				while self.readyTime() < 0.0:
					self.handleQueue.append(self.popHandle())
			elif len(self.lowPriorityHandleQueue) > 0:
				self._pollEventsFast()
				handle = self.lowPriorityHandleQueue.popleft()
				self.handleQueue.append(handle)
			else:
				self._pollEvents(timeToNext)
		handle, args = self.handleQueue.popleft()
		handle(*args)
	
	def _pollEvents(self, timeout):
		if timeout is inf:
			timeout = -1
		keys = self.pollingObject.poll(timeout)
		if keys == []:
			while self.readyTime() < 0:
				self.handleQueue.append(self.popHandle())
		else:
			self._scheduleEvents(keys)
	
	def _pollEventsFast(self):
		""" Used when there is a low priority handle waiting, poll events but
		    only what is currently waiting. I.e. do not use any timeout.
		"""
		keys = self.pollingObject.poll(0)
		if keys != []:
			self._scheduleEvents(keys)
	
	def _scheduleEvents(self, events):
		for activeFD, activeMask in events:
			handleList = self.handles[activeFD]
			for mask,handle in handleList.items():
				mask = (mask | errorCheckingMask) & activeMask
				if mask:
					self.handleQueue.append((handle,(mask,)))
