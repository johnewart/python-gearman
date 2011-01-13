import collections
import time
import logging

from gearman import command_handler
from gearman import constants
from gearman import errors
from gearman import protocol
from gearman.protocol import GEARMAN_COMMAND_JOB_CREATED, GEARMAN_COMMAND_WORK_STATUS, GEARMAN_COMMAND_WORK_COMPLETE, GEARMAN_COMMAND_WORK_FAIL, \
    GEARMAN_COMMAND_STATUS_RES, GEARMAN_COMMAND_ERROR, GEARMAN_COMMAND_WORK_EXCEPTION, GEARMAN_COMMAND_WORK_DATA, GEARMAN_COMMAND_WORK_WARNING

gearman_logger = logging.getLogger(__name__)

EVENT_JOB_DISCONNECTED = 'job_disconnected'
EVENT_JOB_EXCEPTION = protocol.get_command_name(GEARMAN_COMMAND_WORK_EXCEPTION)
EVENT_JOB_DATA = protocol.get_command_name(GEARMAN_COMMAND_WORK_DATA)
EVENT_JOB_WARNING = protocol.get_command_name(GEARMAN_COMMAND_WORK_WARNING)
EVENT_JOB_STATUS = protocol.get_command_name(GEARMAN_COMMAND_WORK_STATUS)
EVENT_JOB_CREATED = protocol.get_command_name(GEARMAN_COMMAND_JOB_CREATED)
EVENT_JOB_COMPLETE = protocol.get_command_name(GEARMAN_COMMAND_WORK_COMPLETE)
EVENT_JOB_FAIL = protocol.get_command_name(GEARMAN_COMMAND_WORK_FAIL)
EVENT_STATUS_RES = protocol.get_command_name(GEARMAN_COMMAND_STATUS_RES)
EVENT_GEARMAN_ERROR = protocol.get_command_name(GEARMAN_COMMAND_ERROR)

class GearmanClientCommandHandler(command_handler.GearmanCommandHandler):
    """Maintains the state of this connection on behalf of a GearmanClient"""
    def _reset_handler(self):
        super(GearmanClientCommandHandler, self)._reset_handler()
        # When we first submit jobs, we don't have a handle assigned yet... these handles will be returned in the order of submission
        self._requests_awaiting_handles = collections.deque()
        self._handle_to_request_map = dict()

    def handle_disconnect(self):
        for pending_request in self._requests_awaiting_handles:
            pending_request.state = constants.JOB_UNKNOWN
            self._notify(EVENT_JOB_DISCONNECTED, pending_request)

        for inflight_request in self._handle_to_request_map.itervalues():
            inflight_request.state = constants.JOB_UNKNOWN
            self._notify(EVENT_JOB_DISCONNECTED, inflight_request)

    ##################################################################
    ##### Public interface methods to be called by GearmanClient #####
    ##################################################################
    def send_job_request(self, current_request):
        """Register a newly created job request"""
        assert not current_request.job.connection and not current_request.job.connection.connected, "We shouldn't already be connected to a server"
        self._assert_request_state(current_request, constants.JOB_UNKNOWN)

        if current_request.connection_attempts >= current_request.max_handler_attempts:
            raise errors.ExceededConnectionAttempts('Exceeded %d connection attempt(s) :: %r' % (current_request.max_handler_attempts, current_request))

        # Once this command is sent, our request needs to wait for a handle
        current_request.job.connection = self
        current_request.connection_attempts += 1

        # Handle the I/O for requesting a job - determine which COMMAND we need to send
        cmd_type = protocol.submit_cmd_for_background_priority(current_request.background, current_request.priority)

        gearman_job = current_request.job
        outbound_data = self.encode_data(gearman_job.data)
        self.send_command(cmd_type, task=gearman_job.task, unique=gearman_job.unique, data=outbound_data)

        current_request.state = constants.JOB_PENDING

        self._requests_awaiting_handles.append(current_request)

    def send_get_status_of_job(self, current_request):
        """Forward the status of a job"""
        self._register_request(current_request)
        self.send_command(protocol.GEARMAN_COMMAND_GET_STATUS, job_handle=current_request.job.handle)

    ##################################################################
    ## Gearman command callbacks with kwargs defined by protocol.py ##
    ##################################################################
    def _assert_request_state(self, current_request, expected_state):
        if current_request.state == expected_state:
            return

        raise errors.InvalidClientState('Expected handle (%s) to be in state %r, got %r' % (current_request.job.handle, expected_state, current_request.state))

    def _register_request(self, current_request):
        self._handle_to_request_map[current_request.job.handle] = current_request

    def _unregister_request(self, current_request):
        # De-allocate this request for all jobs
        return self._handle_to_request_map.pop(current_request.job.handle, None)

    #### Interaction with commands ####
    def recv_error(self, error_code, error_text):
        """When we receive an error from the server, notify the connection manager that we have a gearman error"""
        gearman_logger.error('Received error from server: %s: %s' % (error_code, error_text))
        self._notify(EVENT_GEARMAN_ERROR, error_code, error_text)

    def recv_job_created(self, job_handle):
        if not self._requests_awaiting_handles:
            raise errors.InvalidClientState('Received a job_handle with no pending requests')

        # If our client got a constants.JOB_CREATED, our request now has a server handle
        current_request = self._requests_awaiting_handles.popleft()
        self._assert_request_state(current_request, constants.JOB_PENDING)

        # Update the state of this request
        current_request.job.handle = job_handle
        current_request.state = constants.JOB_CREATED
        self._register_request(current_request)

        self._notify(EVENT_JOB_CREATED, current_request)

    def recv_work_data(self, job_handle, data):
        # Queue a WORK_DATA update
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        current_request.data_updates.append(self.decode_data(data))

        self._notify(EVENT_JOB_DATA, current_request)

    def recv_work_warning(self, job_handle, data):
        # Queue a WORK_WARNING update
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        current_request.warning_updates.append(self.decode_data(data))

        self._notify(EVENT_JOB_WARNING, current_request)

    def recv_work_status(self, job_handle, numerator, denominator):
        # Queue a WORK_STATUS update
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        # The protocol spec is ambiguous as to what type the numerator and denominator is...
        # But according to Eskil, gearmand interprets these as integers
        current_request.status = {
            'handle': job_handle,
            'known': True,
            'running': True,
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }

        self._notify(EVENT_JOB_STATUS, current_request.status)

    def recv_work_complete(self, job_handle, data):
        # Update the state of our request and store our returned result
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        current_request.result = self.decode_data(data)
        current_request.state = constants.JOB_COMPLETE

        self._notify(EVENT_JOB_COMPLETE, current_request)
        self._unregister_request(current_request)

    def recv_work_fail(self, job_handle):
        # Update the state of our request and mark this job as failed
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        current_request.state = constants.JOB_FAILED

        self._notify(EVENT_JOB_FAIL, current_request)
        self._unregister_request(current_request)

    def recv_work_exception(self, job_handle, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self._handle_to_request_map[job_handle]
        self._assert_request_state(current_request, constants.JOB_CREATED)

        current_request.exception = self.decode_data(data)

        self._notify(EVENT_JOB_EXCEPTION, current_request)

    def recv_status_res(self, job_handle, known, running, numerator, denominator):
        # Make our status response Python friendly
        job_known = bool(known == '1')
        status_dict = {
            'handle': job_handle,
            'known': job_known,
            'running': bool(running == '1'),
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }

        self._notify(EVENT_STATUS_RES, status_dict)

        # If we received a STATUS_RES update about this request, update our known status
        current_request = self._handle_to_request_map.get(job_handle)
        if current_request:
            current_request.status = status_dict
            if not job_known:
                self._unregister_request(current_request)
