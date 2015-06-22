# unstuck

Introduction
============

Unstuck is an optimized replacement for asyncio.
It does not, however, provide direct compatibility with asyncio.
The reason for this is that the architecture of asyncio itself is part of the
motivation behind this project.
The main point of contention with asyncio is the inability to seemlessly
transition between coroutines and subroutines.
See the section [below](#aPoints) for details

<a name="aPoints"></a>
Addressed points
================

Again again is verboten
-----------------------

