import logging
import time

from gearman import compat
from gearman import util
from gearman.connection_manager import GearmanConnectionManager
from gearman.admin_handler import AdminConnection, ConnectionError
from gearman.errors import InvalidAdminClientState, ServerUnavailable
from gearman.protocol import GEARMAN_COMMAND_ECHO_RES, GEARMAN_COMMAND_ECHO_REQ, \
    GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, \
    GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN

gearman_logger = logging.getLogger(__name__)

ECHO_STRING = "ping? pong!"
DEFAULT_ADMIN_CLIENT_TIMEOUT = 10.0

class GearmanAdminClient(GearmanConnectionManager):
    """GearmanAdminClient :: Interface to send/receive administrative commands to a Gearman server

    This client acts as a BLOCKING client and each call will poll until it receives a satisfactory server response

    http://gearman.org/index.php?id=protocol
    See section 'Administrative Protocol'
    """
    connection_class = AdminConnection

    def __init__(self, host_list=None, poll_timeout=DEFAULT_ADMIN_CLIENT_TIMEOUT):
        super(GearmanAdminClient, self).__init__(host_list=host_list)
        self.poll_timeout = poll_timeout

        self.admin_handler = util.first(self._handler_pool)

    def wait_until_handler_established(self, poll_timeout=None):
        # Poll to make sure we send out our request for a status update
        self.establish_handlers()

        def continue_while_not_connected():
            return not compat.all(current_handler.connected for current_handler in self._handler_pool)

        self.poll_handlers_until_stopped(continue_while_not_connected, timeout=poll_timeout)

        if not compat.all(current_handler.connected for current_handler in self._handler_pool):
            raise ServerUnavailable(self._handler_pool)

    def ping_server(self):
        """Sends off a debugging string to execute an application ping on the Gearman server"""
        start_time = time.time()

        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_echo_request(ECHO_STRING)
        server_response = self.wait_until_server_responds(GEARMAN_COMMAND_ECHO_REQ)
        if server_response != ECHO_STRING:
            raise InvalidAdminClientState("Echo string mismatch: got %s, expected %s" % (server_response, ECHO_STRING))

        elapsed_time = time.time() - start_time
        return elapsed_time

    def send_maxqueue(self, task, max_size):
        """Sends a request to change the maximum queue size for a given task"""

        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_text_command('%s %s %s' % (GEARMAN_SERVER_COMMAND_MAXQUEUE, task, max_size))
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_MAXQUEUE)

    def send_shutdown(self, graceful=True):
        """Sends a request to shutdown the connected gearman server"""
        actual_command = GEARMAN_SERVER_COMMAND_SHUTDOWN
        if graceful:
            actual_command += ' graceful'

        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_text_command(actual_command)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_SHUTDOWN)

    def get_status(self):
        """Retrieves a list of all registered tasks and reports how many items/workers are in the queue"""
        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_text_command(GEARMAN_SERVER_COMMAND_STATUS)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_STATUS)

    def get_version(self):
        """Retrieves the version number of the Gearman server"""
        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_text_command(GEARMAN_SERVER_COMMAND_VERSION)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_VERSION)

    def get_workers(self):
        """Retrieves a list of workers and reports what tasks they're operating on"""
        self.wait_until_handler_established(poll_timeout=self.poll_timeout)
        self.admin_handler.send_text_command(GEARMAN_SERVER_COMMAND_WORKERS)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_WORKERS)

    def wait_until_server_responds(self, expected_type):
        def continue_while_no_response(any_activity):
            return (not self.admin_handler.response_ready)

        self.poll_handlers_until_stopped([self.admin_handler], continue_while_no_response, timeout=self.poll_timeout)
        if not self.admin_handler.response_ready:
            raise InvalidAdminClientState('Admin client timed out after %f second(s)' % self.poll_timeout)

        cmd_type, cmd_resp = self.admin_handler.pop_response()
        if cmd_type != expected_type:
            raise InvalidAdminClientState('Received an unexpected response... got command %r, expecting command %r' % (cmd_type, expected_type))

        return cmd_resp
