import errno
import logging
import select

import gearman.util
from gearman.constants import _DEBUG_MODE_
from gearman.connection import ConnectionError
from gearman.job import GearmanJob, GearmanJobRequest
from gearman import compat

gearman_logger = logging.getLogger(__name__)

EVENT_READ = 'read'
EVENT_WRITE = 'write'
EVENT_ERROR = 'error'

POLLER_EVENTS = set([EVENT_READ, EVENT_WRITE, EVENT_ERROR])

class ConnectionPoller(object):
    """Abstract base class for any Gearman-type client that needs to current_sockect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Automatically encodes all 'data' fields as specified in protocol.py
    """
    ###################################
    # Connection management functions #
    ###################################

    def __init__(self, event_broker=None):
        self._read_fd_set = set()
        self._write_fd_set = set()
        self._entire_fd_set = set()

        self._event_broker = event_broker

    def register_for_read(self, current_fd):
        """Add a new current_sockection to this current_sockection manager"""
        self._read_fd_set.add(current_fd)
        self._entire_fd_set.add(current_fd)

    def register_for_write(self, current_fd):
        self._write_fd_set.add(current_fd)
        self._entire_fd_set.add(current_fd)

    def unregister(self, current_fd):
        self._read_fd_set.discard(current_fd)
        self._write_fd_set.discard(current_fd)
        self._entire_fd_set.discard(current_fd)

    def _notify(self, poller_event):
        if self._event_broker:
            self._event_broker.notify(self, poller_event, *args, **kwargs)

    def notify_read(self, fd):
        self._notify(EVENT_READ, fd)

    def notify_write(self, fd):
        self._notify(EVENT_WRITE, fd)

    def notify_error(self, fd):
        self._notify(EVENT_ERROR, fd)

    def poll_until_stopped(self, continue_polling_callback, timeout=None):
        countdown_timer = gearman.util.CountdownTimer(timeout)

        callback_ok = continue_polling_callback()
        timer_ok = bool(not countdown_timer.expired)

        while bool(callback_ok and timer_ok):
            self.poll_once(timeout=countdown_timer.time_remaining)
  
            callback_ok = continue_polling_callback()
            timer_ok = bool(not countdown_timer.expired)

        # Return True, if we were stopped by our callback
        return bool(not callback_ok)

    def poll_once(self, timeout=None):
        raise NotImplementedError

class SelectPoller(ConnectionPoller):
    def poll_once(self, timeout=None):
        # Do a single robust select and handle all current_sockection activity
        read_fds, write_fds, ex_fds = self._select_poll_once(timeout=timeout)

        for current_fd in read_fds:
            self.notify_read(current_fd)

        for current_fd in write_fds:
            self.notify_write(current_fd)

        for current_fd in ex_fds:
            self.notify_error(current_fd)

    def _select_poll_once(self, timeout=None):
        """Does a single robust select, catching socket errors"""
        check_rd_fds = set(self._read_fd_set)
        check_wr_fds = set(self._write_fd_set)
        check_ex_fds = set(self._entire_fd_set)

        actual_rd_fds = set()
        actual_wr_fds = set()
        actual_ex_fds = set()
        if timeout is not None and timeout < 0.0:
            return actual_rd_fds, actual_wr_fds, actual_ex_fds

        successful_select = False
        remainings_fds = set(self._entire_fd_set)
        while not successful_select and compat.any([check_rd_fds, check_rd_fds, check_ex_fds]):
            try:
                event_rd_fds, event_wr_fds, event_ex_fds = self.execute_select(check_rd_fds, check_wr_fds, check_ex_fds, timeout=timeout)
                actual_rd_fds |= set(event_rd_fds)
                actual_wr_fds |= set(event_wr_fds)
                actual_ex_fds |= set(event_ex_fds)

                successful_select = True
            except select.error:
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://docs.python.org/howto/sockets
                for fd_to_test in remainings_fds:
                    try:
                        _, _, _ = self.execute_select([fd_to_test], [], [], timeout=0.0)
                    except select.error:
                        check_rd_fds.discard(fd_to_test)
                        check_wr_fds.discard(fd_to_test)
                        check_ex_fds.discard(fd_to_test)
                        gearman_logger.error('select error: %r' % current_sock_to_test)

        if _DEBUG_MODE_:
            gearman_logger.debug('select :: Poll - %d :: Read - %d :: Write - %d :: Error - %d', \
                len(self._entire_fd_set), len(actual_rd_fds), len(actual_wr_fds), len(actual_ex_fds))

        return actual_rd_fds, actual_wr_fds, actual_ex_fds

    @staticmethod
    def execute_select(rd_fds, wr_fds, ex_fds, timeout=None):
        """Behave similar to select.select, except ignoring certain types of exceptions"""
        select_args = [rd_fds, wr_fds, ex_fds]
        if timeout is not None:
            select_args.append(timeout)

        try:
            rd_list, wr_list, er_list = select.select(*select_args)
        except select.error, exc:
            # Ignore interrupted system call, reraise anything else
            if exc[0] != errno.EINTR:
                raise

            rd_list = []
            wr_list = []
            er_list = []

        return rd_list, wr_list, er_list

GearmanConnectionPoller = SelectPoller