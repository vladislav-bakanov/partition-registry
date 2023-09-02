class NotRegisteredSourceError(Exception):
    ...


class NotPositiveIntervalError(ValueError):
    ...


class UnknownSourceError(ValueError):
    ...


class IncorrectSourceNameError(ValueError):
    ...


class UnknownPartitionStrategyError(ValueError):
    ...
    

class UnknownPartitionTypeError(ValueError):
    ...


class IncorrectProviderNameError(ValueError):
    ...


class DifferentEventsWithTheSameTimestampError(ValueError):
    ...
