from . import Event, Catalogue, _listify
from .filter import Predicate
from .orm import backend

from typing import Union, List


def get_catalogues(base: Union[Event, Predicate] = None) -> List[Catalogue]:
    return backend.get_catalogues(base)


def get_events(base: Union[Catalogue, Predicate] = None) -> List[Event]:
    return backend.get_events(base)


def save(instances: List[Union[Event, Catalogue]]) -> None:
    for instance in _listify(instances):
        if isinstance(instance, Event):
            backend.save_event(instance)
        elif isinstance(instance, Catalogue):
            backend.save_catalogue(instance)
        else:
            raise ValueError('Can only create or update Events or Catalogues.')
    backend.commit()
