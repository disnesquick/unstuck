# unstuck

Introduction
============

Unstuck is an optimized replacement for asyncio.
It does not, however, provide direct compatibility with asyncio.
The reason for this is that the architecture of asyncio itself is part of the
motivation behind this project.
The main point of contention with asyncio is the inability to seemlessly
transition between coroutines and subroutines.
See the section [below](#addressed-points) for details.
Unstuck is currently very much work-in-progress and you should expect to find
uncommented/unformatted code, as well as somewhat shakey exception handling.


Motivation
==========

Unstuck was begun due to discovered limitations in asyncio during the creation
of the python side of the Ripley project. Networking functions have therefore
been prioritised.


Addressed points
================

Again again is verboten
-----------------------

Asyncio allows one to block on a coroutine or Future by using the
`get_event_loop().run_until_complete(coro)` function call. A coroutine can then
call back into a standard subroutine but, from then on, it is not possible to
wait on a coroutine or Future with the event loop. The standard solution to this
problem is to ensure that ALL code after a coroutine call is thereafter a
coroutine but this is obviously not an ideal solution. An "obvious" solution
would be to make the event loop stackable, i.e. a new loop frame would be
created on secondary "awaits". However, if two processes are interleaved then
there is a possibility to have a consistently growing stack with the lower
elements having been completed long ago but control being unable to be
resumed until the top of the stack is cleared. The unstuck solution is to use
greenlets where stacked awaits would normally occur.

bloat
-----

