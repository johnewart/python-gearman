import collections
from gearman import compat
import logging
import os
import random

from gearman import util
from gearman import connection_manager
from gearman import client_handler
from gearman import job

from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_UNKNOWN, JOB_PENDING, JOB_COMPLETE, JOB_FAILED

gearman_logger = logging.getLogger(__name__)

# This number must be <= GEARMAN_UNIQUE_SIZE in gearman/libgearman/constants.h 
RANDOM_UNIQUE_BYTES = 16

class GearmanClient(connection_manager.ConnectionManager):
    """
    GearmanClient :: Interface to submit jobs to a Gearman server
    """
    job_class = job.GearmanJob
    job_request_class = job.GearmanJobRequest
    command_handler_class = client_handler.GearmanClientCommandHandler

    _retry_on_failure = False

    def __init__(self, host_list=None, random_unique_bytes=RANDOM_UNIQUE_BYTES):
        super(GearmanClient, self).__init__(host_list=host_list)

        self._random_unique_bytes = random_unique_bytes

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self._request_to_handler_queue = {}

        self._requests_pending_created = set()
        self._requests_pending_data = set()
        self._requests_pending_warning = set()
        self._requests_pending_complete = set()
        self._requests_pending_fail = set()
        self._requests_pending_exception = set()
        self._jobs_pending_status = set()

        self._client_managed_sets = set([
            id(self._requests_pending_created),
            id(self._requests_pending_data),
            id(self._requests_pending_warning),
            id(self._requests_pending_complete),
            id(self._requests_pending_fail),
            id(self._jobs_pending_status),
        ])


    def submit_job(self, task, data, unique=None, priority=PRIORITY_NONE, background=False, wait_until_complete=True, max_retries=0, poll_timeout=None):
        """Submit a single job to any gearman server"""
        job_info = dict(task=task, data=data, unique=unique, priority=priority)

        completed_job_list = self.submit_multiple_jobs([job_info], background=background, wait_until_complete=wait_until_complete, max_retries=max_retries, poll_timeout=poll_timeout)

        return util.unlist(completed_job_list)

    def submit_multiple_jobs(self, jobs_to_submit, background=False, wait_until_complete=True, max_retries=0, poll_timeout=None):
        """Takes a list of jobs_to_submit with dicts of

        {'task': task, 'data': data, 'unique': unique, 'priority': priority}
        """
        assert type(jobs_to_submit) in (list, tuple, set), "Expected multiple jobs, received 1?"

        # Convert all job dicts to job request objects
        requests_to_submit = [self._create_request_from_dictionary(job_info, background=background, max_retries=max_retries) for job_info in jobs_to_submit]

        return self.submit_multiple_requests(requests_to_submit, wait_until_complete=wait_until_complete, poll_timeout=poll_timeout)

    def submit_multiple_requests(self, job_requests, wait_until_complete=True, poll_timeout=None):
        """Take GearmanJobRequests, assign them connections, and request that they be done.

        * Optionally blocks until our jobs are accepted (should be fast) OR times out
        * Optionally blocks until jobs are all complete

        You MUST check the status of your requests after calling this function as "timed_out" or "state == JOB_UNKNOWN" maybe True
        """
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"

        for current_request in job_requests:
            self._register_request(current_request)
            self._send_job_request(current_request)

        if wait_until_complete:
            job_requests = self.wait_until_jobs_completed(job_requests, poll_timeout=poll_timeout)
        else:
            job_requests = self.wait_until_jobs_accepted(job_requests, poll_timeout=poll_timeout)

        return job_requests

    def wait_until_jobs_accepted(self, job_requests, poll_timeout=None):
        return self._wait_while_requests_pending(job_requests, self._requests_pending_created, poll_timeout=poll_timeout)

    def wait_until_jobs_completed(self, job_requests, poll_timeout=None):
        return self._wait_while_requests_pending(job_requests, self._requests_pending_complete, poll_timeout=poll_timeout)

    def _wait_while_requests_pending(self, job_requests, pending_request_set, poll_timeout=None):
        """Go into a select loop until all our jobs have moved to STATE_PENDING"""
        if type(job_requests) not in (tuple, list, set):
            raise TypeError("'job_requests' must be of type (tuple, list, set)")
        elif type(pending_request_set) != set:
            raise TypeError("'pending_request_set' must be of type (set)")
        elif id(pending_request_set) not in self._client_managed_sets:
            raise ValueError("'pending_request_set' NOT being managed by this GearmanClient")

        # Promote our iterable of job_requests to a set for fast comparisons in the future
        known_request_set = set(job_requests)

        # Add known requests to a "pending_request_set" automatically checked in self._on_job_*
        pending_request_set |= known_request_set

        # Poll until we know our request is no longer pending (it's been evicted from the pending_request_set)
        def continue_while_pending():
            return bool(known_request_set & pending_request_set)

        self._poll_until_stopped(continue_while_pending, timeout=poll_timeout)

        # Mark any job still in the queued state to poll_timeout
        for current_request in job_requests:
            current_request.timed_out = bool(current_request in pending_request_set)

        # Evict our requests once we know we can longer wait for them
        pending_request_set -= known_request_set
        return job_requests
    # 
    # def get_job_status(self, current_request, poll_timeout=None):
    #     """Fetch the job status of a single request"""
    #     request_list = self.get_job_statuses([current_request], poll_timeout=poll_timeout)
    #     return gearman.util.unlist(request_list)
    # 
    # def get_job_statuses(self, job_requests, poll_timeout=None):
    #     """Fetch the job status of a multiple requests"""
    #     assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
    #     countdown_timer = gearman.util.CountdownTimer(poll_timeout)
    # 
    #     self.wait_until_handler_established(poll_timeout=countdown_timer.time_remaining)
    # 
    #     for current_request in job_requests:
    #         current_request.status['last_time_received'] = current_request.status.get('time_received')
    # 
    #         current_handler = current_request.job.connection
    #         current_handler.send_get_status_of_job(current_request)
    # 
    #     return self.wait_until_job_statuses_received(job_requests, poll_timeout=countdown_timer.time_remaining)
    # 
    # def wait_until_job_statuses_received(self, job_requests, poll_timeout=None):
    #     """Go into a select loop until we received statuses on all our requests"""
    #     assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
    #     def is_status_not_updated(current_request):
    #         current_status = current_request.status
    #         return bool(current_status.get('time_received') == current_status.get('last_time_received'))
    # 
    #     # Poll to make sure we send out our request for a status update
    #     def continue_while_status_not_updated():
    #         for current_request in job_requests:
    #             if is_status_not_updated(current_request) and current_request.state != JOB_UNKNOWN:
    #                 return True
    # 
    #         return False
    # 
    #     self._poll_until_stopped(continue_while_status_not_updated, timeout=poll_timeout)
    # 
    #     for current_request in job_requests:
    #         current_request.status = current_request.status or {}
    #         current_request.timed_out = is_status_not_updated(current_request)
    # 
    #     return job_requests
    # 
    # def wait_until_handler_established(self, connection_set, poll_timeout=None):
    #     for current_handler in self._handler_pool:
    #         if current_handler.disconnected:
    #             current_handler.connect()
    # 
    #     # Poll to make sure we send out our request for a status update
    #     self._poll_once(timeout=0.0)
    # 
    #     if not compat.any(current_handler.connected for current_handler in self._handler_pool):
    #         raise errors.ServerUnavailable(self._handler_pool)

    def _register_request(self, current_request):
        """When registering a request, keep track of which connections we've already tried submitting to"""
        if current_request in self._request_to_handler_queue:
            return

        # Always start off this task on the same server if possible
        shuffled_connections = random.shuffle(list(self._connection_pool))
        self._request_to_handler_queue[current_request] = collections.deque(shuffled_connections)

    def _unregister_request(self, current_request):
        self._request_to_handler_queue.pop(current_request, None)

    def _create_request_from_dictionary(self, job_info, background=False, max_retries=0):
        """Takes a dictionary with fields  {'task': task, 'unique': unique, 'data': data, 'priority': priority, 'background': background}"""
        # Make sure we have a unique identifier for ALL our tasks
        job_unique = job_info.get('unique')
        if job_unique == '-':
            job_unique = job_info['data']
        elif not job_unique:
            job_unique = os.urandom(self._random_unique_bytes).encode('hex')

        current_job = self.job_class(connection=None, handle=None, task=job_info['task'], unique=job_unique, data=job_info['data'])

        initial_priority = job_info.get('priority', PRIORITY_NONE)

        max_attempts = max_retries + 1
        current_request = self.job_request_class(current_job, initial_priority=initial_priority, background=background, max_attempts=max_attempts)
        return current_request

    def _send_job_request(self, current_request):
        """Attempt to send out a job request"""
        chosen_handler = self._choose_connection(current_request)
        chosen_handler.send_job_request(current_request)

        current_request.timed_out = False
        return current_request

    def _choose_connection(self, current_request):
        """Return a live connection for the given hash"""
        # We'll keep track of the connections we're attempting to use so if we ever have to retry, we can use this history
        rotating_connections = self._request_to_handler_queue[current_request]

        failed_connections = 0
        chosen_connection = None
        for possible_connection in rotating_connections:
            if possible_connection.connected:
                chosen_connection = possible_connection
                break

            failed_connections += 1

        # Rotate our server list so we'll skip all our broken servers
        rotating_connections.rotate(-failed_connections)
        return chosen_connection

    ###################################
    ##### Event handler functions #####
    ###################################
    def _on_job_disconnect(self, current_handler, current_request):
        self._on_job_fail(current_request)

    def _on_job_created(self, current_handler, current_request):
        self._requests_pending_created.discard(current_request)

    def _on_job_status(self, current_handler, current_request):
        pass

    def _on_job_data(self, current_handler, current_request):
        self._requests_pending_data.discard(current_request)

    def _on_job_warning(self, current_handler, current_request):
        self._requests_pending_warning.discard(current_request)

    def _on_job_complete(self, current_handler, current_request):
        self._requests_pending_complete.discard(current_request)
        self._unregister_request(current_request)

    def _on_job_fail(self, current_handler, current_request):
        self._requests_pending_fail.discard(current_request)
        if self._retry_on_failure:
            self._send_job_request(current_request)
        else:
            self._unregister_request(current_request)

    def _on_job_exception(self, current_handler, current_request):
        self._requests_pending_exception.discard(current_request)

    def _on_status_update(self, current_handler, job_info):
        pass

    def _on_gearman_error(self, current_handler, *args, **kwargs):
        pass

    ###################################
    # Command handler mgmt functions ##
    ###################################
    def _setup_command_handler(self, current_handler):
        super(GearmanClient, self)._setup_command_handler(current_handler)
        self.register_for_event(self._on_job_disconnect, client_handler.EVENT_JOB_DISCONNECTED, current_handler)
        self.register_for_event(self._on_job_exception, client_handler.EVENT_JOB_EXCEPTION, current_handler)
        self.register_for_event(self._on_job_data, client_handler.EVENT_JOB_DATA, current_handler)
        self.register_for_event(self._on_job_warning, client_handler.EVENT_JOB_WARNING, current_handler)
        self.register_for_event(self._on_job_status, client_handler.EVENT_JOB_STATUS, current_handler)
        self.register_for_event(self._on_job_complete, client_handler.EVENT_JOB_COMPLETE, current_handler)
        self.register_for_event(self._on_job_fail, client_handler.EVENT_JOB_FAIL, current_handler)
        self.register_for_event(self._on_status_update, client_handler.EVENT_STATUS_RES, current_handler)
        self.register_for_event(self._on_gearman_error, client_handler.EVENT_GEARMAN_ERROR, current_handler)
        return current_handler

    def _teardown_command_handler(self, current_handler):
        self.unregister_for_event(self._on_gearman_error, client_handler.EVENT_GEARMAN_ERROR, current_handler)
        self.unregister_for_event(self._on_status_update, client_handler.EVENT_STATUS_RES, current_handler)
        self.unregister_for_event(self._on_job_fail, client_handler.EVENT_JOB_FAIL, current_handler)
        self.unregister_for_event(self._on_job_complete, client_handler.EVENT_JOB_COMPLETE, current_handler)
        self.unregister_for_event(self._on_job_status, client_handler.EVENT_JOB_STATUS, current_handler)
        self.unregister_for_event(self._on_job_warning, client_handler.EVENT_JOB_WARNING, current_handler)
        self.unregister_for_event(self._on_job_data, client_handler.EVENT_JOB_DATA, current_handler)
        self.unregister_for_event(self._on_job_exception, client_handler.EVENT_JOB_EXCEPTION, current_handler)
        self.unregister_for_event(self._on_job_disconnect, client_handler.EVENT_JOB_DISCONNECTED, current_handler)
        super(GearmanClient, self)._teardown_command_handler(current_handler)
        return current_handler

