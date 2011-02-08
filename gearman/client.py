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

EVENT_AWAITING_VALID_CONNECTION = 'awaiting_valid_connection'

# I have a ConnectionManager M
# M = ConnectionManager(gearmanconfig = {'host' list of host ports)})
# I want to call jobs named "do_foo"
# I inherit from something and make something that handles foo jobs result/fails
#
# class FooHandler(GearmanSpecificAPI):
#      def on_fail(conn)
#      def on_success(conn,result)
#      def on_io_fail(conn,...)
# 

cm = ConnectionManager()
for command_handler in all_handlers:
	cm._register_for_event(my_callback, ON_FAILURE, current_handlre)


# I'm reading data from someone who connected to my port 80. I need to do foo.
# handler = FooHandler()
# conn = M.send_job ('foo', blob, handler, blocking=True)
# fd = conn.fileno

# IOloopmethod
# ... ioloop (fd)
# if fd is read|write...
# M.on_read(fd) | M.on_write(fd)

# M.handle(conn)

# that should cause calls into handler

#    def send_my_info(self, task_name, job_blob)
#        fd -> connection
#        connection -> handler [state machine]
#        handler.send_job_request()


class GearmanClient(object):
    """
    GearmanClient :: Interface to submit jobs to a Gearman server
    """
    job_class = job.GearmanJob
    job_request_class = job.GearmanJobRequest
    command_handler_class = client_handler.GearmanClientCommandHandler

    _retryon_failure = False

    def __init__(self, host_list=None, random_unique_bytes=RANDOM_UNIQUE_BYTES):
        self._connection_manager = connection_manager.ConnectionManager(host_list=host_list)

        self._random_unique_bytes = random_unique_bytes

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self._request_to_handler_queue = {}

        self._requests_awaiting_attempt = collections.deque()
        self._requests_in_flight = set()

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
            self._requests_awaiting_attempt.append(current_request)

        assert not self._requests_in_flight, "Cannot have multiple objects in flight"
        self._requests_in_flight = job_requests

        if wait_until_complete:
            pending_event = client_handler.EVENT_JOB_COMPLETE
        else:
            pending_event = client_handler.EVENT_JOB_ACCEPTED

        self._register_for_event(evict_request_when_done, pending_event, current_handler)

        # Enter IOLoop and fire off a bunch of events
        self._connection_manager.start_polling(timeout=poll_timeout)

        self._unregister_for_event(evict_request_when_done, pending_event, current_handler)

        for current_request in self._requests_in_flight:
            current_request.timeout = True

        self._requests_in_flight = set()

        return job_requests

    def _evict_request_when_done(self, current_handler, current_request):
        self._requests_in_flight.discard(current_request)
        if not self._requests_in_flight:
            self._connection_manager.stop_polling()

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

    def wait_until_connection_available(self, poll_timeout=None):
        # Poll until we know our request is no longer pending (it's been evicted from the pending_request_set)
        def continue_while_any_pending():
            return not any(current_connection.connected for current_connection in self._connection_pool)

        self._poll_until_stopped(continue_while_any_pending, timeout=poll_timeout)

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
        chosen_handler = self._choose_handler(current_request)
        chosen_handler.send_job_request(current_request)
        current_request.timed_out = False
        return current_request

    def _choose_handler(self, current_request):
        """Return a live connection for the given hash"""
        # We'll keep track of the connections we're attempting to use so if we ever have to retry, we can use this history
        rotating_handlers = self._request_to_handler_queue[current_request]

        failed_handlers = 0
        chosen_handler = None
        for possible_handler in rotating_handlers:
            if possible_handler.live:
                chosen_handler = possible_handler
                break

            failed_handlers += 1

        # Rotate our server list so we'll skip all our broken servers
        rotating_handlers.rotate(-failed_handlers)
        if not chosen_handler:
            raise Exception(self._connection_manager.handlers)

        return chosen_handler

    def _register_request(self, current_request):
        """When registering a request, keep track of which connections we've already tried submitting to"""
        if current_request in self._request_to_handler_queue:
            return

        # Always start off this task on the same server if possible
        shuffled_handlers = list(self._connection_manager.handlers)
        random.shuffle(shuffled_handlers)
        self._request_to_handler_queue[current_request] = collections.deque(shuffled_handlers)

        self._notify_awaiting_job_attempt(current_request)

    def _unregister_request(self, current_request):
        self._request_to_handler_queue.pop(current_request, None)

    ###################################
    ##### Event handler functions #####
    ###################################
    def on_job_disconnect(self, current_handler, current_request):
        pass

    def on_job_exception(self, current_handler, current_request):
        pass

    def on_job_data(self, current_handler, current_request):
        pass

    def on_job_warning(self, current_handler, current_request):
        pass

    def on_job_status(self, current_handler, current_request):
        pass

    def on_job_complete(self, current_handler, current_request):
        self._requests_pending_complete.discard(current_request)
        self._unregister_request(current_request)

    def on_job_fail(self, current_handler, current_request):
        self._requests_pending_fail.discard(current_request)
        if self._retryon_failure:
            self._notify_awaiting_job_attempt(current_request)
        else:
            self._unregister_request(current_request)

    def on_status_update(self, current_handler, job_info):
        pass

    def on_gearman_error(self, current_handler, *args, **kwargs):
        pass

    def _register_for_event(self, callback_fxn, event_name, event_source):
        """Call 'callback_fxn' on 'event_name' coming from 'event_source'"""
        self._connection_manager.register_for_event(event_source, event_name, callback_fxn)

    def _unregister_for_event(self, callback_fxn, event_name, event_source):
        self._connection_manager.unregister_for_event(event_source, event_name, callback_fxn)

    ###################################
    # Command handler mgmt functions ##
    ###################################
    def setup_event_listeners(self, command_handler, command_handler=None):
        self._register_for_event(self.on_job_disconnect, client_handler.EVENT_JOB_DISCONNECTED, command_handler)
        self._register_for_event(self.on_job_exception, client_handler.EVENT_JOB_EXCEPTION, command_handler)
        self._register_for_event(self.on_job_data, client_handler.EVENT_JOB_DATA, command_handler)
        self._register_for_event(self.on_job_warning, client_handler.EVENT_JOB_WARNING, command_handler)
        self._register_for_event(self.on_job_status, client_handler.EVENT_JOB_STATUS, command_handler)
        self._register_for_event(self.on_job_complete, client_handler.EVENT_JOB_COMPLETE, command_handler)
        self._register_for_event(self.on_job_fail, client_handler.EVENT_JOB_FAIL, command_handler)
        self._register_for_event(self.on_status_update, client_handler.EVENT_STATUS_RES, command_handler)
        self._register_for_event(self.on_gearman_error, client_handler.EVENT_GEARMAN_ERROR, command_handler)

    def teardown_event_listeners(self, connection_manager, command_handler=None):
        self._unregister_for_event(self.on_gearman_error, client_handler.EVENT_GEARMAN_ERROR, command_handler)
        self._unregister_for_event(self.on_status_update, client_handler.EVENT_STATUS_RES, command_handler)
        self._unregister_for_event(self.on_job_fail, client_handler.EVENT_JOB_FAIL, command_handler)
        self._unregister_for_event(self.on_job_complete, client_handler.EVENT_JOB_COMPLETE, command_handler)
        self._unregister_for_event(self.on_job_status, client_handler.EVENT_JOB_STATUS, command_handler)
        self._unregister_for_event(self.on_job_warning, client_handler.EVENT_JOB_WARNING, command_handler)
        self._unregister_for_event(self.on_job_data, client_handler.EVENT_JOB_DATA, command_handler)
        self._unregister_for_event(self.on_job_exception, client_handler.EVENT_JOB_EXCEPTION, command_handler)
        self._unregister_for_event(self.on_job_disconnect, client_handler.EVENT_JOB_DISCONNECTED, command_handler)
