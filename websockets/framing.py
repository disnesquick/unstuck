import collections
import io
import random
import struct
try:
	import numpy
except:
	numpy = None
from .errors import *

inf = float("inf")

OP_CONT, OP_TEXT, OP_BINARY = range(0x00, 0x03)
OP_CLOSE, OP_PING, OP_PONG = range(0x08, 0x0b)

CLOSE_CODES = {
    1000: "OK",
    1001: "going away",
    1002: "protocol error",
    1003: "unsupported type",
    # 1004: - (reserved)
    # 1005: no status code (internal)
    # 1006: connection closed abnormally (internal)
    1007: "invalid data",
    1008: "policy violation",
    1009: "message too big",
    1010: "extension required",
    1011: "unexpected error",
    # 1015: TLS failure (internal)
}

WSHEAD_FIN_BIT  = 0b1000000000000000
WSHEAD_RES_BITS = 0b0111000000000000
WSHEAD_OP_BITS  = 0b0000111100000000
WSHEAD_MASK_BIT = 0b0000000010000000
WSHEAD_LEN_BITS = 0b0000000001111111



def applyMaskSlow(maskBits, data):
	return bytes(b ^ maskBits[i & 3] for i, b in enumerate(data))


def applyMaskFast(aa,bb):
	rlen = len(bb)
	a=numpy.frombuffer(aa,dtype=numpy.uint32)
	b=numpy.frombuffer(bb,dtype=numpy.uint32,count=int(len(bb)/4))
	main = numpy.bitwise_xor(a,b)
	if rlen & 3:
		rem = bb[rlen&~3:]
		rem = bytes(d ^ m for d,m in zip(rem,aa))
		return main.tostring() + rem
	else:
		return main.tostring()

if numpy:
	applyMask = applyMaskFast
else:
	applyMask = applyMaskSlow

def readFragment(socket, mask, maxSize = inf):
	# Read the header
	data = yield from socket.recv(2)
	head, = struct.unpack('!H', data)
	
	finalFragment = bool(head & WSHEAD_FIN_BIT)
	
	if head & WSHEAD_RES_BITS:
	    raise WebSocketProtocolError("Reserved bits must be 0")
	
	opcode = (head & WSHEAD_OP_BITS) >> 8
	
	if bool(head & WSHEAD_MASK_BIT) != mask:
	    raise WebSocketProtocolError("Incorrect masking")
	
	# Grab the length component of the header
	length = head & WSHEAD_LEN_BITS
	if length == 126:
		data = yield from socket.recv(2)
		length, = struct.unpack('!H', data)
	elif length == 127:
		data = yield from socket.recv(8)
		length, = struct.unpack('!Q', data)
	
	# Ensure that the length falls below maximum fragment size
	if length > maxSize:
		raise PayloadTooBig("Payload exceeds limit "
		                    "(%d > %d bytes)" % (length, maxSize))

	# If the header specifies then read the masking component
	if mask:
		maskBits = yield from socket.recv(4)
	
	# Read the data
	data = yield from socket.recv(length)
	if mask:
	    data = applyMask(maskBits, data)
	
	if opcode in (OP_CLOSE, OP_PING, OP_PONG):
		if len(data) > 125:
			raise WebSocketProtocolError("Control frame too long")
		if not finalFragment:
			raise WebSocketProtocolError("Fragmented control frame")
	elif opcode in (OP_CONT, OP_TEXT, OP_BINARY):
		pass
	else:
		raise WebSocketProtocolError("Invalid opcode")
	
	return opcode, data, finalFragment


def writeFragment(socket, mask, opcode, data, final):
	length = len(data)
	
	head1 = 0b10000000 if final else 0
	head1 |= opcode
	head2 = 0b10000000 if mask else 0
	
	if length < 0x7e:
		header = struct.pack('!BB', head1, head2 | length)
	elif length < 0x10000:
		header = struct.pack('!BBH', head1, head2 | 126, length)
	else:
		header = struct.pack('!BBQ', head1, head2 | 127, length)
	
	if mask:
		maskBits = struct.pack('!I', random.getrandbits(32))
		header = header + maskBits
		data = applyMask(maskBits, data)
	yield from socket.send(header+data)
	#yield from socket.send(data)


def parseCloseData(data):
	""" Parse the data in a close frame.
	
	    Return (code, reason)
	"""
	length = len(data)
	if length == 0:
		return 1005, ''
	
	elif length == 1:
		raise WebSocketProtocolError("Close frame too short")
	
	else:
		code, = struct.unpack('!H', data[:2])
		if not (code in CLOSE_CODES or 3000 <= code < 5000):
			raise WebSocketProtocolError("Invalid status code")
		reason = data[2:].decode('utf-8')
		return code, reason
