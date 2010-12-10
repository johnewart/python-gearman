class GearmanError(Exception):
    pass

class ConnectionError(GearmanError):
    pass

class ServerUnavailable(GearmanError):
    pass

class ProtocolError(GearmanError):
    pass

class UnknownCommandError(GearmanError):
    pass

# Deprecated since 2.0.2, removing in next major release
class ExceededConnectionAttempts(GearmanError):
    pass

# Added in 2.0.2, successor to ExceededConnectionAttempts
class ExceededSubmissionAttempts(ExceededConnectionAttempts):
    pass

class InvalidClientState(GearmanError):
    pass

class InvalidWorkerState(GearmanError):
    pass

class InvalidAdminClientState(GearmanError):
    pass
