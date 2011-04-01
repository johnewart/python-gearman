import logging

from gearman import constants
from gearman import errors
from gearman import util
from gearman import protocol

gearman_logger = logging.getLogger(__name__)

STATE_LIVE = 'live'
STATE_DEAD = 'dead'

EVENT_DATA_READ = 'data_read'
EVENT_DATA_SEND = 'data_send'

class GearmanCommandHandler(object):
    _is_server_side = False
    _is_client_side = True

    AVAILABLE_EVENTS = None

    def __init__(self, data_encoder=None, event_broker=None):
        self._command_callback_map = {}

        self._data_encoder = data_encoder
        self._event_broker = event_broker
        self.reset()

    def handle_connect(self):
        self._state = STATE_LIVE

    def handle_disconnect(self):
        self._state = STATE_DEAD

    def reset(self):
        self._state = STATE_DEAD

    @property
    def live(self):
        return bool(self._state == STATE_LIVE)

    @property
    def dead(self):
        return bool(self._state == STATE_LIVE)

    #### Interaction with the raw socket ####
    def _notify(self, command_event, *args, **kwargs):
        self._explicit_notify(self, command_event, *args, **kwargs)

    def _explicit_notify(self, event_source, event_name, *args, **kwargs):
        if self._event_broker:
            self._event_broker.notify(event_source, event_name, *args, **kwargs)

    def recv_data(self, data_stream):
        current_data_stream = data_stream
        bytes_read = 0

        while True:
            cmd_type, cmd_args, cmd_len = self._unpack_command(current_data_stream)
            if cmd_type == protocol.GEARMAN_COMMAND_NO_COMMAND:
                break

            self.recv_command(cmd_type, cmd_args)

            current_data_stream = current_data_stream[cmd_len:]
            bytes_read += cmd_len

        self._notify(EVENT_DATA_READ, bytes_read=bytes_read)

    def recv_command(self, cmd_type, cmd_args):
        """Reads data from socket --> buffer"""
        # Cache our callback code so we can get improved QPS
        cmd_callback = self._locate_command_callback(cmd_type, cmd_args)

        # Expand the arguments as passed by the protocol
        # This must match the parameter names as defined in the command handler
        cmd_callback(**cmd_args)

    def send_command(self, cmd_type, **cmd_args):
        data_stream = self._pack_command(cmd_type, cmd_args)
        self.send_data(data_stream)

    def send_data(self, data_stream):
        self._notify(EVENT_DATA_SEND, data_stream=data_stream)

    def encode_data(self, data):
        """Convenience function :: handle object -> binary string packing"""
        return self._data_encoder.encode(data)

    def decode_data(self, data):
        """Convenience function :: handle binary string -> object unpacking"""
        return self._data_encoder.decode(data)

    #### Interaction with commands ####
    def _locate_command_callback(self, cmd_type, cmd_args):
        cmd_callback = self._command_callback_map.get(cmd_type)
        if cmd_callback:
            return cmd_callback

        gearman_command_name = protocol.get_command_name(cmd_type)
        if bool(gearman_command_name == cmd_type) or not gearman_command_name.startswith('command_'):
            unknown_command_msg = 'Could not handle command: %r - %r' % (gearman_command_name, cmd_args)
            gearman_logger.error(unknown_command_msg)
            raise ValueError(unknown_command_msg)

        recv_command_function_name = gearman_command_name.lower().replace('command_', 'recv_')

        cmd_callback = getattr(self, recv_command_function_name, None)
        if not cmd_callback:
            missing_callback_msg = 'Could not handle command: %r - %r' % (protocol.get_command_name(cmd_type), cmd_args)
            gearman_logger.error(missing_callback_msg)
            raise errors.UnknownCommandError(missing_callback_msg)

        self._command_callback_map[cmd_type] = cmd_callback
        return cmd_callback

    def _pack_command(self, cmd_type, cmd_args):
        """Converts a command to its raw binary format"""
        if cmd_type not in protocol.GEARMAN_PARAMS_FOR_COMMAND:
            raise errors.ProtocolError('Unknown command: %r' % protocol.get_command_name(cmd_type))

        if constants._DEBUG_MODE_:
            gearman_logger.debug('%s - Send - %s - %r', hex(id(self)), protocol.get_command_name(cmd_type), cmd_args)

        if cmd_type == protocol.GEARMAN_COMMAND_TEXT_COMMAND:
            return protocol.pack_text_command(cmd_type, cmd_args)
        else:
            # We'll be sending a response if we know we're a server side command
            is_response = bool(self._is_server_side)
            return protocol.pack_binary_command(cmd_type, cmd_args, is_response)

    def _unpack_command(self, given_buffer):
        """Conditionally unpack a binary command or a text based server command"""
        if not given_buffer:
            cmd_type = protocol.GEARMAN_COMMAND_NO_COMMAND
            cmd_args = {}
            cmd_len = 0
        elif given_buffer[0] == protocol.NULL_CHAR:
            # We'll be expecting a response if we know we're a client side command
            is_response = bool(self._is_client_side)
            cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(given_buffer, is_response=is_response)
        else:
            cmd_type, cmd_args, cmd_len = protocol.parse_text_command(given_buffer)

        if constants._DEBUG_MODE_ and cmd_type != protocol.GEARMAN_COMMAND_NO_COMMAND:
            gearman_logger.debug('%s - Recv - %s - %r', hex(id(self)), protocol.get_command_name(cmd_type), cmd_args)

        return cmd_type, cmd_args, cmd_len
