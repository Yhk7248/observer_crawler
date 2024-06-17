from enum import Enum, unique, auto


@unique
class PeriodType(Enum):
    Y = auto()
    M = auto()
    W = auto()
    D = auto()
