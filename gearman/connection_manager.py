import logging
import select as select_lib

import gearman.util
from gearman.connection import ASocket
from gearman.connection_poller import GearmanConnectionPoller
from gearman.connection import ConnectionError
from gearman.constants import _DEBUG_MODE_
from gearman.errors import ServerUnavailable
from gearman.job import GearmanJob, GearmanJobRequest
from gearman import protocol
from gearman import compat

gearman_logger = logging.getLogger(__name__)

class DataEncoder(object):
    @classmethod
    def encode(cls, encodable_object):
        raise NotImplementedError

    @classmethod
    def decode(cls, decodable_string):
        raise NotImplementedError

class NoopEncoder(DataEncoder):
    """Provide common object dumps for all communications over gearman"""
    @classmethod
    def _enforce_byte_string(cls, given_object):
        if type(given_object) != str:
            raise TypeError("Expecting byte string, got %r" % type(given_object))

    @classmethod
    def encode(cls, encodable_object):
        cls._enforce_byte_string(encodable_object)
        return encodable_object

    @classmethod
    def decode(cls, decodable_string):
        cls._enforce_byte_string(decodable_string)
        return decodable_string

class GearmanConnectionManager(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards all communication between a connection and a command handler
    The state of a connection is represented within the command handler

    Automatically encodes all 'data' fields as specified in protocol.py
    """
    connection_class = connection.ASocket
    command_handler_class = None
    connection_poller_class = GearmanConnectionPoller

    data_encoder = NoopEncoder

    def __init__(self, host_list=None):
        assert self.connection_class is not None, 'GearmanClientBase did not receive a connection class'

        self._poller = self.connection_poller_class()

        host_list = host_list or []
        for hostport_tuple in host_list:
            self.connect_to_host(hostport_tuple)

        self._connection_pool = set()
        self._fd_to_connection_map = {}

        self._handler_to_connection_map = {}
        self._connection_to_handler_map = {}

    def connect_to_host(self, hostport_tuple):
        """Add a new connection to this connection manager"""
        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)
        gearman_port = gearman_port or constants.DEFAULT_GEARMAN_PORT

        current_connection = self.connection_class(host=gearman_host, port=gearman_port)

        self._connection_pool.add(current_connection)

        current_fd = current_connection.fileno()

        self._poller.register_for_read(current_fd)
        self._poller.register_for_write(current_fd)

        # Establish a connection immediately - check for socket exceptions like: "host not found"
        current_connection.connect()

        return current_connection

    def shutdown(self):
        # Shutdown all our connections one by one
        for current_connection in self._fd_to_connection_map.itervalues():
            try:
                current_connection.close()
            except ConnectionError:
                pass

    def on_poller_read(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]
        try:
            current_connection.handle_read()
        except ConnectionError:
            self.on_poller_error(poller, fd)

    def on_poller_write(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]
        try:
            current_connection.handle_write()
        except ConnectionError:
            self.on_poller_error(poller, fd)

    def on_poller_error(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]

        current_connection.handle_error()

    def on_connection_established(self, current_connection):
        current_handler = self.command_handler_class(data_encoder=self.data_encoder)

        self._connection_to_handler_map[current_connection] = current_handler
        self._handler_to_connection_map[current_handler] = current_connection

        util.call_on_event(self.on_connection_read, current_connection, connection.EVENT_PENDING_READ)
        util.call_on_event(self.on_command_write, current_handler, command_handler.EVENT_DATA_SEND)

        current_handler.setup()

        return current_socket

    def on_connection_lost(self, current_connection):
        current_handler = self._connection_to_handler_map[current_connection]

        current_handler.teardown()

        current_fd = current_connection.fileno()

        self._poller.unregister(current_fd)

        return current_socket

    def on_connection_read(self, current_connection, data_stream=None):
        current_handler = self._connection_to_handler_map[current_connection]

        data_stream = current_connection.peek()

        current_handler.recv_data(data_stream)

    def on_command_write(self, current_handler, data_stream=None):
        assert data_stream is not None, "Missing data_stream"
        
        current_connection = self._handler_to_connection_map[current_connection]

        current_connection.send(data_stream)

    def on_gearman_error(self, current_handler):
        return current_handler

    ###################################
    # Connection management functions #
    ###################################

    def _register_with_poller(self, current_connection):
        # Once we have a socket, register this connection with the poller
        current_fd = current_connection

        self._fd_to_connection_map[current_connection.fileno()] = current_connection

        return current_connection

    def _unregister_with_poller(self, current_connection):
        self._connection_pool.discard(current_handler)

        return current_handler

    def _poll_until_stopped(self, continue_polling_callback, timeout=None):
        return self._poller.poll_until_stopped(continue_polling_callback, timeout=timeout)

    def _poll_once(self, timeout=None):
        return self._poller.poll_once(timeout=timeout)
