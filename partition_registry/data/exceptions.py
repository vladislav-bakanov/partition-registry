class NotRegisteredSourceError(Exception):
    ...


class NotPositiveIntervalError(ValueError):
    ...


class UnknownSourceError(ValueError):
    ...
    

class UnknownSourceTypeError(ValueError):
    ...


class UnknownProviderError(ValueError):
    ...


class IncorrectSourceNameError(ValueError):
    ...


class UnknownPartitionStrategyError(ValueError):
    ...


class UnknownEventStateError(ValueError):
    ...


class IncorrectProviderNameError(ValueError):
    ...


class DifferentEventsWithTheSameTimestampError(ValueError):
    ...
