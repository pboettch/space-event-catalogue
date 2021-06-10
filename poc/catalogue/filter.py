import datetime as dt
from typing import List, Union, Literal


class Field:
    def __init__(self, name: str):
        self.value = name


class Attribute:
    def __init__(self, name: str):
        self.value = name


class Condition:
    pass


class Binary(Condition):
    def __init__(self,
                 lhs: Union[Field, Attribute],
                 op: Union[Literal['>'], Literal['>='],
                           Literal['<'], Literal['<='],
                           Literal['=='], Literal['!=']],
                 rhs: Union[str, int, float, dt.datetime, bool]):
        pass


class Match(Condition):
    def __init__(self,
                 lhs: Union[Field, Attribute],
                 rhs: str):  # regex
        pass


class Not(Condition):
    def __init__(self, rhs: Condition):
        pass


class Has(Condition):
    def __init__(self, rhs: Attribute):
        pass


class All:
    def __init__(self, conditions: List[Condition]):
        pass


class Any:
    def __init__(self, conditions: List[Condition]):
        pass
