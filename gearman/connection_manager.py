import logging
import select as select_lib

import gearman.util
from gearman.connection import ASocket
from gearman.poller import GearmanConnectionPoller, POLLER_EVENTS
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
    command_handler_class = None
    connection_class = connection.GearmanConnection
    poller_class = connection_poller.GearmanConnectionPoller

    data_encoder = NoopEncoder

    def __init__(self, host_list=None, event_broker=None):
        self._event_broker = event_broker or util.EventBroker()

        self._connection_pool = set()
        self._fd_to_connection_map = {}

        self._handler_to_connection_map = {}
        self._connection_to_handler_map = {}

        self._poller = self._build_poller()
        self._setup_poller(self._poller)

        host_list = host_list or []
        for hostport_tuple in host_list:
            self.connect_to_host(host_list)

    def connect_to_host(self, hostport_tuple):
        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)
        gearman_port = gearman_port or constants.DEFAULT_GEARMAN_PORT

        current_connection = self._build_connection(host=gearman_host, port=gearman_port)
        current_connection = self._setup_connection(current_connection)

        current_handler = self._build_command_handler()
        current_handler = self._setup_command_handler(current_handler)

        self._setup_connection_relation(current_connection, current_handler)

    def shutdown(self):
        all_connections = tuple(self._connection_pool)
        for current_connection in all_connections:
            current_handler = self._connection_to_handler_map[current_connection]
            self._teardown_connection_relation(current_connection, current_handler)

            self._teardown_command_handler(current_handler)

            self._teardown_connection(current_connection)

        self._teardown_poller(self._poller)

    def register_for_event(self, callback_fxn, event_name, event_source):
        """Call 'callback_fxn' on 'event_name' coming from 'event_source'"""
        self._event_broker.listen(event_source, event_name, callback_fxn)

    def unregister_for_event(self, callback_fxn, event_name, event_source):
        self._event_broker.unlisten(event_source, event_name, callback_fxn)

    ###################################
    ##### Event handler functions #####
    ###################################
    def _on_poller_read(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]
        try:
            current_connection.handle_read()
        except connection.ConnectionError:
            self._on_poller_error(poller, fd)

    def _on_poller_write(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]
        try:
            current_connection.handle_write()
        except connection.ConnectionError:
            self._on_poller_error(poller, fd)

    def _on_poller_error(self, poller, fd):
        current_connection = self._fd_to_connection_map[fd]

        current_connection.handle_error()

    def _on_connection_established(self, current_connection):
        current_handler = self._connection_to_handler_map[current_connection]
        current_handler.handle_connect()

    def _on_connection_lost(self, current_connection):
        current_handler = self._connection_to_handler_map[current_connection]
        current_handler.handle_disconnect()

        self._teardown_connection(current_connection)

        current_connection.reset()

        self._setup_connection(current_connection)

    def _on_connection_read(self, current_connection, data_stream=None):
        current_handler = self._connection_to_handler_map[current_connection]

        data_stream = current_connection.peek()

        current_handler.recv_data(data_stream)

    def _on_command_write(self, current_handler, data_stream=None):
        assert data_stream is not None, "Missing data_stream"
        
        current_connection = self._handler_to_connection_map[current_handler]

        current_connection.send(data_stream)

    ###################################
    ### Poller management functions ###
    ###################################
    def _build_poller(self):
        return self.poller_class(event_broker=self._event_broker)

    def _setup_poller(self, current_poller):
        self.register_for_event(self._on_poller_read, EVENT_READ, current_poller)
        self.register_for_event(self._on_poller_write, EVENT_WRITE, current_poller)
        self.register_for_event(self._on_poller_error, EVENT_ERROR, current_poller)
        return current_poller

    def _teardown_poller(self, current_poller):
        self.unregister_for_event(self._on_poller_error, EVENT_ERROR, current_poller)
        self.unregister_for_event(self._on_poller_write, EVENT_WRITE, current_poller)
        self.unregister_for_event(self._on_poller_read, EVENT_READ, current_poller)
        return current_poller

    def _poll_until_stopped(self, continue_polling_callback, timeout=None):
        return self._poller.poll_until_stopped(continue_polling_callback, timeout=timeout)

    def _poll_once(self, timeout=None):
        return self._poller.poll_once(timeout=timeout)

    ###################################
    # Connection management functions #
    ###################################
    def _build_connection(self, host=None, port=None):
        return self.connection_class(host=host, port=port, event_broker=self._event_broker)

    def _setup_connection(self, current_connection):
        """Add a new connection to this connection manager"""
        assert current_connection not in self._connection_pool, "Connection already known: %r" % current_connection

        # Clean up all poller-support structures
        current_fd = current_connection.fileno()

        self._fd_to_connection_map[current_fd] = current_connection
        self._poller.register_for_read(current_fd)
        self._poller.register_for_write(current_fd)

        # Establish a connection immediately - check for socket exceptions like: "host not found"
        current_connection.connect()

        self.register_for_event(self._on_connection_established, connection.EVENT_CONNECTED, current_connection)
        self.register_for_event(self._on_connection_read, connection.EVENT_PENDING_READ, current_connection)
        self.register_for_event(self._on_connection_lost, connection.EVENT_DISCONNECTED, current_connection)

        self._connection_pool.add(current_connection)

        return current_connection

    def _teardown_connection(self, current_connection):
        assert current_connection in self._connection_pool, "Connection not known: %r" % current_connection
        self._connection_pool.discard(current_connection)

        self.unregister_for_event(self._on_connection_lost, connection.EVENT_DISCONNECTED, current_connection)
        self.unregister_for_event(self._on_connection_read, connection.EVENT_PENDING_READ, current_connection)
        self.unregister_for_event(self._on_connection_established, connection.EVENT_CONNECTED, current_connection)

        old_fd = current_connection.fileno()

        current_connection.close()

        self._poller.unregister(old_fd)
        del self._fd_to_connection_map[old_fd]

        return current_connection

    ###################################
    # Command handler mgmt functions ##
    ###################################
    def _build_command_handler(self):
        return self.command_handler_class(data_encoder=self.data_encoder, event_broker=self._event_broker)

    def _setup_command_handler(self, current_handler):
        self.register_for_event(self._on_command_write, command_handler.EVENT_DATA_SEND, current_handler)
        return current_handler

    def _teardown_command_handler(self, current_handler):
        self.unregister_for_event(self._on_command_write, command_handler.EVENT_DATA_SEND, current_handler)
        return current_handler

    ###################################
    ##### Relation mgmt functions #####
    ###################################
    def _setup_connection_relation(self, current_connection, current_handler):
        self._connection_to_handler_map[current_connection] = current_handler
        self._handler_to_connection_map[current_handler] = current_connection

    def _teardown_connection_relation(self, current_connection, current_handler):
        del self._connection_to_handler_map[current_connection]
        del self._handler_to_connection_map[current_handler]

