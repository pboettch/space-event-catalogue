import re
import datetime as dt

from typing import Dict, List, Union

from uuid import uuid4

from .filter import Predicate

_valid_key = re.compile(r'^[A-Za-z][A-Za-z_0-9]*$')


def _listify(v):
    if type(v) in [list, tuple]:
        return v
    else:
        return [v]


def _create_attributes(inst: object, kwargs: Dict) -> None:
    for k, v in kwargs.items():
        if _valid_key.match(k):
            inst.__dict__[k] = v
        else:
            raise ValueError('Invalid key-name for event-meta-data in kwargs:', k)


class _BackendBasedEntity:
    def representation(self, name: str) -> str:
        fix = ', '.join(k + '=' + str(self.__dict__[k]) for k in self._fixed_keys)
        kv = ', '.join(k + '=' + str(v) for k, v in self.variable_attributes_as_dict().items())
        return f'{name}({fix}) attributes({kv})'

    def variable_attributes_as_dict(self):
        ret = {}
        for k, v in self.__dict__.items():
            if k in self._fixed_keys:
                continue
            if _valid_key.match(k):
                ret[k] = v
        return ret


class Event(_BackendBasedEntity):
    _fixed_keys = ['start', 'end', 'author', 'uuid']

    def __init__(self, start: dt.datetime, end: dt.datetime, author: str, uuid=None, **kwargs):
        super().__init__()

        self.start = start
        self.end = end
        self.author = author

        if not uuid:
            self.uuid = str(uuid4())
        else:
            # TODO check if uuid is a valid UUID
            self.uuid = uuid

        _create_attributes(self, kwargs)

    def __repr__(self):
        return self.representation('Event')


class Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author', 'predicate']

    def __init__(self, name: str, author: str,
                 predicate: Predicate = None,
                 events: List[Event] = None,
                 **kwargs):
        super().__init__()

        self.name = name
        self.author = author
        self.predicate = predicate

        if events:
            self._added_events = events
        else:
            self._added_events = []
        self._removed_events = []

        _create_attributes(self, kwargs)

    def add_events(self, events: Union[Event, List[Event]]):
        self._added_events += _listify(events)

    def remove_events(self, events: Union[Event, List[Event]]):
        self._removed_events += _listify(events)

    def __repr__(self):
        return self.representation('Catalogue')
