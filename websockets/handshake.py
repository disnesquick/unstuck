import re
import sys
import base64
import hashlib
import random

from .errors import *

__all__ = ["clientHandshake", "serverHandshake"]

# global constants
WEBSOCKETS_VERSION = "2.4"
MAX_HEADERS = 256
MAX_LINE = 4096
USER_AGENT = (' '.join((
    'Python/{}'.format(sys.version[:3]),
    'websockets/{}'.format(WEBSOCKETS_VERSION),
))).encode()
WEBSOCKETS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

def clientHandshake(socket, host, resourceName = "/", origin = None,
	                        subprotocols = None):
	""" Perform the client side of the opening handshake.
	
	    If provided, `origin' sets the HTTP Origin header.
	    If provided, `subprotocols' is a list of supported subprotocols, in
	    order of decreasing preference.
	"""
	host = host.encode()
	
	if origin is not None:
		origin = origin.encode()
	
	if subprotocols is not None:
		encSubprotocols = (', '.join(subprotocols)).encode()
	else:
		encSubprotocols = None
	
	resourceName = resourceName.encode()
	
	key, request = buildRequest(host, resourceName, origin, encSubprotocols)
	request.append(b"\r\n")
	
	request = b'\r\n'.join(request)
	# Send the request...
	yield socket.send(request)
	
	# ...and read the response
	try:
		status, headers = yield from readResponse(socket.reader)
	except Exception as exc:
		raise InvalidHandshake("Malformed HTTP message") from exc
	
	if status != 101:
		raise InvalidHandshake("Bad status code: %d" % status)
	
	checkResponse(headers, key)
	
	subprotocol = headers.get(b"Sec-WebSocket-Protocol", None)
	if subprotocol is not None:
		subprotocol = subprotocol.decode()
		if subprotocol not in subprotocols:
			raise InvalidHandshake("Unknown subprotocol: %s" % subprotocol)
	
	return subprotocol


def serverHandshake(socket, origins=None, subprotocols=None):
	""" Perform the server side of the opening handshake.
	    If provided, `origins` is a list of acceptable HTTP Origin values.
	    Include ``''`` if the lack of an origin is acceptable. If provided,
	    `subprotocols` is a list of supported subprotocols.
	
	    Return the URI of the request.
	"""
	# Read handshake request.
	try:
		path, headers = yield from readRequest(socket.reader)
	except Exception as exc:
		raise InvalidHandshake("Malformed HTTP message") from exc
	# Ensure that it is a valid websocket header and grab the key
	key = checkRequest(headers)
	
	if origins is not None:
		origin = headers[b'Origin']
		if not set(origin.split() or ['']) <= set(origins):
			raise InvalidHandshake("Bad origin: {}".format(origin))
	
	# TODO: subprotocol selection
	subprotocol = None
	if subprotocols is not None:
		protocol = header.get(b"Sec-WebSocket-Protocol", None)
		if protocol is not None:
			clientSPs = [p.strip().decode() for p in protocol.split(b',')]
			subprotocol = selectSubprotocol(clientSPs, subprotocols).decode()
	
	response = buildResponse(key, subprotocol)
	response = b'\r\n'.join(response)
	yield from socket.send(response)
	
	return path.decode(), subprotocol 


def buildRequest(host, resourceName, origin, subprotocols):
	""" Build a handshake request to send to the server.
	    Return the `key` which must be passed to :func:`check_response`.
	"""
	rand = bytes(random.getrandbits(8) for _ in range(16))
	key = base64.b64encode(rand)
	
	request = [b"GET " + resourceName + b" HTTP/1.1"]
	request.append(b"Host: " + host)
	
	if origin is not None:
		request.append(b"Origin: " + origin)
	
	if subprotocols is not None:
		request.append(b'Sec-WebSocket-Protocol: ' + subprotocols)
	
	request.append(b"User-Agent: " + USER_AGENT)
	request.append(b"Upgrade: WebSocket")
	request.append(b"Connection: Upgrade")
	request.append(b"Sec-WebSocket-Key: " + key)
	request.append(b"Sec-WebSocket-Version: 13")

	return key, request


def readRequest(stream):
	""" Stolen from aaugustin
	
	    Read an HTTP/1.1 request from `stream`.
	    Return `(path, headers)` where `path` is a :class:`str` and `headers`
	    is a :class:`~email.message.Message`; `path` isn't URL-decoded.
	    Raise an exception if the request isn't well formatted.
	    The request is assumed not to contain a body.
	"""
	requestLine, header = yield from readMessage(stream)
	method, path, version = requestLine[:-2].split(None, 2)
	if method != b"GET":
		raise ValueError("Unsupported method")
	if version != b"HTTP/1.1":
		raise ValueError("Unsupported HTTP version [%s]"%version)
	return path, header


def checkRequest(header):
	""" Stolen from aaugustin

	    Check a handshake request received from the client.
	    If the handshake is valid, this function returns the `key` which must be
	    passed to :func:`build_response`.
	    Otherwise, it raises an :exc:`~websockets.exceptions.InvalidHandshake`
	    exception and the server must return an error, usually 400 Bad Request.
	    This function doesn't verify that the request is an HTTP/1.1 or higher
	    GET request and doesn't perform Host and Origin checks. These controls
	    are usually performed earlier in the HTTP request handling code. They're
	    the responsibility of the caller.
	"""
	try:
		assert header[b'Upgrade'].lower() == b'websocket'
		assert any(token.strip() == b'upgrade'
		        for token in header[b'Connection'].lower().split(b','))
		key = header[b'Sec-WebSocket-Key']
		assert len(base64.b64decode(key)) == 16
		assert header[b'Sec-WebSocket-Version'] == b'13'
		return key
	except (AssertionError, KeyError) as exc:
		raise InvalidHandshake("Invalid request") from exc


def buildResponse(key, subprotocol = None):
	""" Build a handshake response to send to the client.
	    `key` comes from :func:`check_request`.
	"""
	response = [b'HTTP/1.1 101 Switching Protocols', b"Server: "+USER_AGENT]
	if subprotocol is not None:
		response.append(b"Sec-WebSocket-Protocol: "+subprotocol)
	response.append(b"Upgrade: WebSocket")
	response.append(b"Connection: Upgrade")
	response.append(b"Sec-WebSocket-Accept: " + acceptKeyEncode(key))
	response.append(b'\r\n')
	
	return response


def readResponse(stream):
	""" Read an HTTP/1.1 response from `stream'.
	
	    Return `(status, headers)` where `status` is a :class:`int` and
	    `headers` is a :class:`~email.message.Message`.
	    Raise an exception if the request isn't well formatted.
	    The response is assumed not to contain a body.
	"""
	statusLine, header = yield from readMessage(stream)
	version, status, reason = statusLine[:-2].split(None, 2)
	if version != b"HTTP/1.1":
		raise ValueError("Unsupported HTTP version %s" % version)
	return int(status), header


def checkResponse(header, key):
	""" Check a handshake response received from the server.
	    `key` comes from :func:`build_request`.
	    If the handshake is valid, this function returns ``None``.
	    Otherwise, it raises an :exc:`~websockets.exceptions.InvalidHandshake`
	    exception.
	    This function doesn't verify that the response is an HTTP/1.1 or higher
	    response with a 101 status code. These controls are the responsibility
	    of the caller.
	"""
	try:
		assert header[b"Upgrade"].lower() == b"websocket"
		assert any(token.strip() == b"upgrade"
		        for token in header[b"Connection"].lower().split(b','))
		assert header[b"Sec-WebSocket-Accept"] == acceptKeyEncode(key)
	except (AssertionError, KeyError) as exc:
		raise InvalidHandshake("Invalid response") from exc


def acceptKeyEncode(key):
	sha1 = hashlib.sha1((key + WEBSOCKETS_GUID)).digest()
	return base64.b64encode(sha1)


def readMessage(stream):
	""" Read an HTTP message from `stream`.
	    Return `(start_line, headers)` where `start_line` is :class:`bytes` and
	    `headers` is a :class:`~email.message.Message`.
	    The message is assumed not to contain a body.
	"""
	requestLine = yield from stream.readline()
	header = {}
	for num in range(MAX_HEADERS):
		headerLine = yield from stream.readline()
		if headerLine == b'\r\n':
			break
		key, value = parseHeaderLine(headerLine)
		header[key] = value
	else:
		raise ValueError("Too many headers")
	return requestLine, header


headerRE = re.compile(b"(?P<name>.*?): (?P<value>.*?)\r\n")
def parseHeaderLine(byteBuf):
	match = headerRE.match(byteBuf)
	if not match:
		raise ValueError("Malformed header line")
	return match.groups()
