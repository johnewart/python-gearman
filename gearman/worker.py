import logging
import random
import sys

from gearman import compat
from gearman.connection_manager import GearmanConnectionManager
from gearman.worker_handler import WorkerConnection, ConnectionError
from gearman import compat
import gearman.util

gearman_logger = logging.getLogger(__name__)

POLL_TIMEOUT_IN_SECONDS = 60.0

class GearmanWorker(GearmanConnectionManager):
    """
    GearmanWorker :: Interface to accept jobs from a Gearman server
    """
    connection_class = WorkerConnection

    def __init__(self, host_list=None):
        self._worker_client_id = None
        self._worker_abilities = {}

        super(GearmanWorker, self).__init__(host_list=host_list)

        self._handler_holding_job_lock = None

    ########################################################
    ##### Public methods for general GearmanWorker use #####
    ########################################################
    def register_task(self, task, callback_function):
        """Register a function with this worker

        def function_callback(calling_gearman_worker, current_job):
            return current_job.data
        """
        self._worker_abilities[task] = callback_function
        self._update_abilities()

        return task

    def unregister_task(self, task):
        """Unregister a function with worker"""
        self._worker_abilities.pop(task, None)
        self._update_abilities()

        return task

    def _update_abilities(self):
        for current_handler in self._handler_pool:
            current_handler.set_abilities(self._worker_abilities.keys())

            if current_handler.connected:
                current_handler.send_abilities()

    def set_client_id(self, client_id):
        """Notify the server that we should be identified as this client ID"""
        self._worker_client_id = client_id
        for current_handler in self._handler_pool:
            current_handler.set_client_id(self._worker_client_id)

            if current_handler.connected:
                current_handler.send_client_id()

        return client_id

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely, complete tasks from all connections."""
        countdown_timer = gearman.util.CountdownTimer(poll_timeout)

        # Shuffle our connections after the poll timeout
        while True:
            print "Entering work loop with %f - %r" % (countdown_timer.time_remaining, self._handler_pool)
            print "%r" % (self._handler_pool, )

            self.establish_handlers()

            self.wait_until_handler_established(poll_timeout=countdown_timer.time_remaining)

            self.wait_until_handler_lost(poll_timeout=countdown_timer.time_remaining)

            self.after_poll()

            print "%r" % (self._handler_pool, )

            if compat.all(current_handler.disconnected for current_handler in self._handler_pool):
                raise Exception

            countdown_timer.reset()

    def wait_until_handler_lost(self, poll_timeout=None):
        def continue_while_handlers_alive():
            return not bool(self._dead_handlers)

        self.poll_handlers_until_stopped(continue_while_handlers_alive, timeout=poll_timeout)

    def shutdown(self):
        self._handler_holding_job_lock = None
        super(GearmanWorker, self).shutdown()

    ###############################################################
    ## Methods to override when dealing with connection polling ##
    ##############################################################
    def after_poll(self):
        """Polling callback to notify any outside listeners whats going on with the GearmanWorker.

        Return True to continue polling, False to exit the work loop"""
        return True

    def handle_error(self, current_handler):
        """If we discover that a connection has a problem, we better release the job lock"""
        self.set_job_lock(current_handler, lock=False)
        super(GearmanWorker, self).handle_error(current_handler)

    #############################################################
    ## Public methods so Gearman jobs can send Gearman updates ##
    #############################################################
    def _get_handler_for_job(self, current_job):
        return self.connection_to_handler_map[current_job.connection]

    def wait_until_updates_sent(self, multiple_gearman_jobs, poll_timeout=None):
        connection_set = set([current_job.connection for current_job in multiple_gearman_jobs])
        def continue_while_updates_pending(any_activity):
            return compat.any(current_handler.writable() for current_handler in connection_set)

        self.poll_handlers_until_stopped(connection_set, continue_while_updates_pending, timeout=poll_timeout)

    def send_job_status(self, current_job, numerator, denominator, poll_timeout=None):
        """Send a Gearman JOB_STATUS update for an inflight job"""
        current_job.connection.send_job_status(current_job, numerator=numerator, denominator=denominator)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_complete(self, current_job, data, poll_timeout=None):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_complete(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_failure(self, current_job, poll_timeout=None):
        """Removes a job from the queue if its backgrounded"""
        current_job.connection.send_job_failure(current_job)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_exception(self, current_job, data, poll_timeout=None):
        """Removes a job from the queue if its backgrounded"""
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_job.connection.send_job_exception(current_job, data=data)
        current_job.connection.send_job_failure(current_job)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_data(self, current_job, data, poll_timeout=None):
        """Send a Gearman JOB_DATA update for an inflight job"""
        current_job.connection.send_job_data(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_warning(self, current_job, data, poll_timeout=None):
        """Send a Gearman JOB_WARNING update for an inflight job"""
        current_job.connection.send_job_warning(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    #####################################################
    ##### Callback methods for GearmanWorkerHandler #####
    #####################################################
    def create_job(self, current_handler, job_handle, task, unique, data):
        """Create a new job using our self.job_class"""
        return self.job_class(current_handler, job_handle, task, unique, data)

    def on_job_execute(self, current_job):
        try:
            function_callback = self._worker_abilities[current_job.task]
            job_result = function_callback(self, current_job)
        except Exception:
            return self.on_job_exception(current_job, sys.exc_info())

        return self.on_job_complete(current_job, job_result)

    def on_job_exception(self, current_job, exc_info):
        self.send_job_failure(current_job)
        return False

    def on_job_complete(self, current_job, job_result):
        self.send_job_complete(current_job, job_result)
        return True

    def set_job_lock(self, current_handler, lock):
        """Set a worker level job lock so we don't try to hold onto 2 jobs at anytime"""
        failed_lock = bool(lock and self._handler_holding_job_lock is not None)
        failed_unlock = bool(not lock and self._handler_holding_job_lock != current_handler)

        # If we've already been locked, we should say the lock failed
        # If we're attempting to unlock something when we don't have a lock, we're in a bad state
        if failed_lock or failed_unlock:
            return False

        if lock:
            self._handler_holding_job_lock = current_handler
        else:
            self._handler_holding_job_lock = None

        return True

    def check_job_lock(self, current_handler):
        """Check to see if we hold the job lock"""
        return bool(self._handler_holding_job_lock == current_handler)
