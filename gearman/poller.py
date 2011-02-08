import errno
import logging
import select

from gearman.constants import _DEBUG_MODE_
from gearman import compat
from gearman import util

gearman_logger = logging.getLogger(__name__)

STATE_RUNNING = 'running'
STATE_STOPPED = 'stopped'

EVENT_READ = 'read'
EVENT_WRITE = 'write'
EVENT_ERROR = 'error'
POLLER_EVENTS = set([EVENT_READ, EVENT_WRITE, EVENT_ERROR])

FD that has seen a read

def poll():
	self.notify_read(fd)

class ConnectionPoller(object):
    """Abstract base class for any Gearman-type client that needs to current_sockect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Automatically encodes all 'data' fields as specified in protocol.py
    """
    ###################################
    # Connection management functions #
    ###################################

    def __init__(self, event_broker=None):
        self._event_fd_map = compat.defaultdict(set)
        self._event_broker = event_broker

    def register(self, current_fd, event_type):
        """Add a new current_sockection to this current_sockection manager"""
        assert event_type in POLLER_EVENTS
        self._event_fd_map[event_type].add(current_fd)

    def unregister(self, current_fd, event_type=None):
        if event_type:
            unregistered_events = [event_type]
        else:
            unregistered_events = POLLER_EVENTS

        for current_event in unregistered_events:
            self._event_fd_map[current_event].discard(current_fd)

    def _notify(self, poller_event, fd):
        if self._event_broker:
            self._event_broker.notify(self, poller_event, fd)

    # Automatically promote fd -> connection -> request
    def notify_read(self, fd):
        self._notify(EVENT_READ, fd)

    def notify_write(self, fd):
        self._notify(EVENT_WRITE, fd)

    def notify_error(self, fd):
        self._notify(EVENT_ERROR, fd)

    def start(self, timeout=None):
        self._state = STATE_RUNNING

        countdown_timer = util.CountdownTimer(timeout)
        while self.running and not countdown_timer.expired:
            self.poll(timeout=countdown_timer.time_remaining)

        self._state = STATE_STOPPED

    def stop(self):
        self._state = STATE_STOPPED

    def poll(self, timeout=None):
        raise NotImplementedError

    @property
    def running(self):
        return bool(self._state == STATE_RUNNING)

    @property
    def stopped(self):
        return bool(self._state == STATE_STOPPED)

class SelectPoller(ConnectionPoller):
    def poll(self, timeout=None):
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
        check_rd_fds = set(self._event_fd_map[EVENT_READ])
        check_wr_fds = set(self._event_fd_map[EVENT_WRITE])
        check_ex_fds = set(self._event_fd_map[EVENT_ERROR])
        all_fds = check_rd_fds | check_wr_fds | check_ex_fds

        actual_rd_fds = set()
        actual_wr_fds = set()
        actual_ex_fds = set()
        if timeout is not None and timeout < 0.0:
            return actual_rd_fds, actual_wr_fds, actual_ex_fds

        successful_select = False
        while not successful_select and compat.any([check_rd_fds, check_wr_fds, check_ex_fds]):
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
                remainings_fds = check_rd_fds | check_wr_fds | check_rd_fds
                for fd_to_test in remainings_fds:
                    try:
                        _, _, _ = self.execute_select([fd_to_test], [], [], timeout=0.0)
                    except select.error:
                        check_rd_fds.discard(fd_to_test)
                        check_wr_fds.discard(fd_to_test)
                        check_ex_fds.discard(fd_to_test)
                        gearman_logger.error('select error: %r' % fd_to_test)

        if _DEBUG_MODE_:
            gearman_logger.debug('select :: Poll - %d :: Read - %d :: Write - %d :: Error - %d', \
                len(all_fds), len(actual_rd_fds), len(actual_wr_fds), len(actual_ex_fds))

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

Poller = SelectPoller