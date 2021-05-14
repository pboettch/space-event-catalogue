import re
import datetime as dt

from typing import Dict, List, Union, Set
from pathlib import Path

from uuid import uuid4

from .orm import orm

_backend = orm.Backend(url='sqlite:///' + str(Path.joinpath(Path.home(), '.space-event-catalogue.sqlite')))
_valid_key = re.compile(r'^[a-z][a-z_0-9]*$')
_session = _backend.session()


def _listify(v):
    if type(v) in [list, tuple]:
        return v
    else:
        return [v]


def _deploy_kwargs(inst: object, kwargs: Dict) -> None:
    for k, v in kwargs.items():
        if _valid_key.match(k):
            inst.__dict__[k] = v
        else:
            raise ValueError('Invalid key-name for event-meta-data in kwargs:', k)


class Filter:
    pass


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

    def __init__(self, start: dt.datetime, end: dt.datetime, author: str, uuid=None,
                 backend_orm_entity=None, **kwargs):
        super().__init__()

        if backend_orm_entity and not isinstance(backend_orm_entity, orm.Event):
            raise ValueError("backend-entity needs to be an orm.Event")

        self._orm_entity = backend_orm_entity

        self.start = start
        self.end = end
        self.author = author

        if not uuid:
            self.uuid = str(uuid4())
        else:
            # TODO check if uuid is a valid UUID
            self.uuid = uuid

        _deploy_kwargs(self, kwargs)

    def _save(self, session):
        if self._orm_entity:  # update
            self._orm_entity.start = self.start
            self._orm_entity.end = self.end
            self._orm_entity.author = self.author
            self._orm_entity.uuid = self.uuid
        else:  # insert
            self._orm_entity = orm.Event(self.start, self.end, self.author, self.uuid)

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in self.variable_attributes_as_dict().items():
            self._orm_entity[k] = v

        session.add(self._orm_entity)

    def __repr__(self):
        return self.representation('Event')


class Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author']

    def __init__(self, name: str, author: str, backend_orm_entity=None, **kwargs):
        super().__init__()

        if backend_orm_entity and not isinstance(backend_orm_entity, orm.Catalogue):
            raise ValueError("backend-entity needs to be an orm.Catalogue")

        self._orm_entity: orm.Catalogue = backend_orm_entity

        self.name = name
        self.author = author

        self._added_events = []
        self._removed_events = []

        _deploy_kwargs(self, kwargs)

    def add_events(self, events: Union[Event, List[Event]]):
        self._added_events += _listify(events)

    def remove_events(self, events: Union[Event, List[Event]]):
        self._removed_events += _listify(events)

    def _save(self, session):

        if self._orm_entity:  # update
            self._orm_entity.name = self.name
            self._orm_entity.author = self.author
        else:
            self._orm_entity = orm.Catalogue(self.name, self.author)

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in self.variable_attributes_as_dict().items():
            self._orm_entity[k] = v

        for e in self._removed_events:
            if e._orm_entity:  # event has already been added to the ORM
                self._orm_entity.events.remove(e._orm_entity)
            else:  # event has not yet been added and is about to be added
                if e in self._added_events and e in self._added_events:
                    self._added_events.remove(e)

        for e in self._added_events:
            if not e._orm_entity:
                e._save(session)
            self._orm_entity.events.append(e._orm_entity)

    def __repr__(self):
        return self.representation('Catalogue')


def get_catalogues(base: Union[Event, Filter] = None) -> List[Catalogue]:
    if base:
        if isinstance(base, Filter):
            raise NotImplemented('Filtering is not yet implemented')
        elif isinstance(base, Event):  # catalogues of an Event
            q = _session.query(orm.Catalogue).filter(orm.Catalogue.events.any(id=base._orm_entity.id))
        else:
            raise AttributeError('Invalid instance of given base object.')
    else:
        q = _session.query(orm.Catalogue)

    catalogues = []
    for c in q.all():
        attr = {v.key: v.value for _, v in c.attributes.items()}
        catalogue = Catalogue(c.name, c.author, backend_orm_entity=c, **attr)
        catalogues += [catalogue]

    return catalogues


def get_events(base: Union[Catalogue, Filter] = None) -> List[Event]:
    if base:
        if isinstance(base, Catalogue):
            q = _session.query(orm.Event).filter(orm.Event.catalogues.any(id=base._orm_entity.id))
        elif isinstance(base, Filter):
            raise AttributeError('Filters not yet supported.')
        else:
            raise AttributeError('Invalid instance of given base object.')
    else:
        q = _session.query(orm.Event)

    events = []
    for e in q:
        attr = {v.key: v.value for _, v in e.attributes.items()}
        event = Event(e.start, e.end, e.author, e.uuid, backend_orm_entity=e, **attr)
        events += [event]

    return events


def save(instances: List[Union[Event, Catalogue]]) -> None:
    for obj in _listify(instances):
        if isinstance(obj, Event) or isinstance(obj, Catalogue):
            obj._save(_session)
        else:
            raise ValueError('Can only create or update Events or Catalogues.')
    _session.commit()
