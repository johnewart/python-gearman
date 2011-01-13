import errno
import socket
import struct
import time

from gearman import util

class ConnectionError(Exception):
    pass

ERRNO_DISCONNECTED = -1

STATE_INIT = 'init'
STATE_CONNECTING = 'connecting'
STATE_CONNECTED = 'connected'
STATE_DISCONNECTED = 'disconnected'

_DISCONNECTED_STATES = set(STATE_INIT, STATE_DISCONNECTED)

EVENT_CONNECTED = 'connected'
EVENT_DISCONNECTED = 'disconnected'
EVENT_PENDING_READ = 'pending_read' # Buffer / socket has data to read, no args
EVENT_PENDING_SEND = 'pending_send' # Buffer / socket has data to send, no args
EVENT_DATA_READ = 'data_read'    # Data read off socket AND buffer, data_stream=byte_array processed
EVENT_DATA_SENT = 'data_sent'    # Data written to the socket, data_stream=byte_array sent
CONNECTION_EVENTS = set([EVENT_CONNECTED, EVENT_DISCONNECTED, EVENT_PENDING_READ, EVENT_PENDING_SEND, EVENT_DATA_READ, EVENT_DATA_SENT])

class Connection(object):
    """Provide a nice wrapper around an asynchronous socket:

    * Reconnect-able with a given host (unlike a socket)
    * Python-buffered socket for use in asynchronous applications
    """
    bytes_to_read = 4096

    def __init__(self, host=None, port=None, event_broker=None):
        self._host = host
        self._port = port
        self._event_broker = event_broker

        if host is None:
            raise self._throw_exception(message='no host')

        if port is None:
            raise self._throw_exception(message='no port')

        self._state = STATE_INIT
        self.reset()

    ########################################################
    ##### Public methods to masquerade like a socket #######
    ########################################################
    def accept(self):
        raise NotImplementedError

    def bind(self, socket_address):
        raise NotImplementedError

    def close(self):
        """Shutdown our existing socket and reset all of our connection data"""
        try:
           self._socket.close()
        except socket.error:
            pass

        self._update_state(ERRNO_DISCONNECTED)

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""
        # If we're already in the process of connecting OR already connected, error out
        if not self.disconnected:
            self._throw_exception(message='invalid connection state')

        # Attempt to do an asynchronous connect
        try:
            socket_address = self.getpeername()
            connect_errno = self._socket.connect_ex(socket_address)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        self._update_state(connect_errno)

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        return self._socket.fileno()

    def getpeername(self):
        """Returns the host and port as if this where a AF_INET socket"""
        return (self._host, self._port)

    def peek(self, bufsize=None):
        """Reads data seen on this socket WITHOUT the buffer advancing.  Akin to socket.recv(bufsize, socket.MSG_PEEK)"""
        peek_buffer = None
        if bufsize is None:
            peek_buffer = self._incoming_buffer
        else:
            peek_buffer = self._incoming_buffer[:bufsize]

        return peek_buffer

    def recv(self, bufsize=None):
        """Reads data seen on this socket WITH the buffer advancing.  Akin to socket.recv(bufsize)"""
        recv_buffer = self.peek(bufsize)

        recv_bytes = len(recv_buffer)
        if recv_bytes:
            processed_bytes = self._incoming_buffer[:recv_bytes]
            self._incoming_buffer = self._incoming_buffer[recv_bytes:]
            self._notify(EVENT_DATA_READ, data_stream=processed_bytes)

        return recv_buffer

    def send(self, data_array):
        """Returns the data sent to this buffer.  Akin to socket.send(bufsize)"""
        self._outgoing_buffer += data_array
        self._notify(EVENT_PENDING_SEND)
        return len(data_array)

    ########################################################
    ##### Public methods - checking connection state #######
    ########################################################
    def _notify(self, connection_event, *args, **kwargs):
        if self._event_broker:
            self._event_broker.notify(self, connection_event, *args, **kwargs)

    @property
    def connected(self):
        return bool(self._state == STATE_CONNECTED)

    @property
    def connecting(self):
        return bool(self._state == STATE_CONNECTING)

    @property
    def disconnected(self):
        return bool(self._state in _DISCONNECTED_STATES)

    @property
    def readable(self):
        """Returns True if we might have data to read"""
        return bool(self.connected)

    @property
    def writable(self):
        """Returns True if we have data to write"""
        connected_and_pending_writes = self.connected and bool(self._outgoing_buffer)
        connecting_in_progress = self.connecting
        return bool(connected_and_pending_writes or connecting_in_progress)

    ########################################################
    ##### Public methods - handling socket events ##########
    ########################################################

    def handle_read(self):
        """Reads data from socket --> buffer"""
        if not self.connected:
            self._throw_exception(message='invalid connection state')
        else:
            recv_buffer = self._recv_from_socket()
            if recv_buffer:
                self._notify(EVENT_PENDING_READ)

    def handle_write(self):
        if self.disconnected:
            self._throw_exception(message='invalid connection state')
        elif self.connecting:
            # Check our socket to see what error number we have - See "man connect"
            connect_errno = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            self._update_state(connect_errno)
        else:
            sent_buffer = self._send_to_socket()
            if sent_buffer:
                self._notify(EVENT_DATA_SENT, data_stream=sent_buffer)

    ########################################################
    ############### Private support methods  ###############
    ########################################################
    def _create_socket(self):
        current_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current_socket.setblocking(0)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))
        return current_socket

    def _update_state(self, error_number):
        """Update our connection state based on 'errno' """
        if error_number == 0:
            self._state = STATE_CONNECTED
            self._notify(EVENT_CONNECTED)
        elif error_number == errno.EINPROGRESS:
            self._state = STATE_CONNECTING
        else:
            old_state = self._state
            self._state = STATE_DISCONNECTED
            if old_state != STATE_INIT:
                self._notify(EVENT_DISCONNECTED)

    def reset(self):
        # Reset all our raw data buffers
        self._socket = self._create_socket()

        self._incoming_buffer = ''
        self._outgoing_buffer = ''

    def _recv_from_socket(self):
        """Read data from socket -> buffer, returns read # of bytes"""
        if not self.connected:
            self._throw_exception(message='invalid connection state')

        recv_buffer = ''
        try:
            recv_buffer = self._socket.recv(self.bytes_to_read)
        except socket.error, socket_exception:
            socket_errno, socket_message = socket_exception
            if socket_errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                self._throw_exception(exception=socket_exception)

        self._incoming_buffer += recv_buffer
        return recv_buffer

    def _send_to_socket(self):
        """Send data from buffer -> socket

        Returns remaining size of the output buffer
        """
        if not self.connected:
            self._throw_exception(message='invalid connection state')

        if not self._outgoing_buffer:
            return ''

        try:
            bytes_sent = self._socket.send(self._outgoing_buffer)
        except socket.error, socket_exception:
            socket_errno, socket_message = socket_exception
            if socket_errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                self._throw_exception(exception=socket_exception)

        socket_buffered_output = self._outgoing_buffer[:bytes_sent]
        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return socket_buffered_output

    def _throw_exception(self, message=None, exception=None):
        if exception:
            message = repr(exception)

        rewritten_message = "<%s:%d> %s" % (self._host, self._port, message)
        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<Connection %s:%d state=%s>' % (self._host, self._port, self._state))
