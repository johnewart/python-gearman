#!/usr/bin/env python
"""
Gearman Client Utils
"""
import errno
import inspect
import functools
import select as select_lib
import time

from gearman import compat
from gearman.constants import DEFAULT_GEARMAN_PORT

class EventBroker(object):
    def __init__(self):
        self._event_listeners = compat.defaultdict(lambda: compat.defaultdict(set))

    def listen(self, event_source, event_name, callback_fxn):
        self._event_listeners[event_source][event_name].add(callback_fxn)

    def unlisten(self, event_source, event_name, callback_fxn):
        self._event_listeners[event_source][event_name].discard(callback_fxn)

    def notify(self, event_source, event_name, *args, **kwargs):
        # At time of notification, we should take a snapshot of all current listeners
        # In case a listener mutates this set on the fly
        known_callbacks = tuple(self._event_listeners[event_source][event_name])
        for current_callback in known_callbacks:
            current_callback(event_source, *args, **kwargs)

class CountdownTimer(object):
    """Timer class that keeps track of time remaining"""
    def __init__(self, requested_seconds):
        self._requested_seconds = requested_seconds
        self._stop_time = None
        self.reset()

    def reset(self):
        if self._requested_seconds is not None:
            self._stop_time = time.time() + self._requested_seconds
        else:
            self._stop_time = None

    @property
    def time_remaining(self):
        if self._stop_time is None:
            return None

        current_time = time.time()
        if self._check_expired(current_time):
            return 0.0

        time_remaining = self._stop_time - current_time
        return time_remaining

    @property
    def expired(self):
        return self._check_expired(time.time())

    def _check_expired(self, given_time):
        if self._stop_time is None:
            return False

        return bool(given_time >= self._stop_time)

def disambiguate_server_parameter(hostport_tuple):
    """Takes either a tuple of (address, port) or a string of 'address:port' and disambiguates them for us"""
    if type(hostport_tuple) is tuple:
        gearman_host, gearman_port = hostport_tuple
    elif ':' in hostport_tuple:
        gearman_host, gearman_possible_port = hostport_tuple.split(':')
        gearman_port = int(gearman_possible_port)
    else:
        gearman_host = hostport_tuple
        gearman_port = DEFAULT_GEARMAN_PORT

    return gearman_host, gearman_port

def unlist(given_list):
    """Convert the (possibly) single item list into a single item"""
    list_size = len(given_list)
    if list_size == 0:
        return None
    elif list_size == 1:
        return given_list[0]
    else:
        raise ValueError(list_size)

def first(given_iterable):
    for first_item in given_iterable:
        return first_item
