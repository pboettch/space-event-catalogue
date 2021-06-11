from . import orm

from .. import Event, Catalogue
from ..filter import Predicate

from pathlib import Path

import pickle

from typing import Union, List

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class _Backend:
    def __init__(self, url: str):
        # self.engine = create_engine(url, echo=True)
        self.engine = create_engine(url)
        orm.Base.metadata.create_all(self.engine)

        self.session = Session(bind=self.engine)

    def save_catalogue(self, catalogue: Catalogue):
        serialized_predicate = pickle.dumps(catalogue.predicate, protocol=3)
        entity = getattr(catalogue, "_backend_entity", None)

        if entity:  # update
            entity.name = catalogue.name
            entity.author = catalogue.author
            entity.predicate = serialized_predicate
        else:
            entity = orm.Catalogue(catalogue.name, catalogue.author, serialized_predicate)
            catalogue._backend_entity = entity

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in catalogue.variable_attributes_as_dict().items():
            entity[k] = v

        for e in catalogue._removed_events:
            event_entity = getattr(e, "_backend_entity", None)
            if event_entity:  # event has already been added to the ORM
                entity.events.remove(event_entity)
            else:  # event has not yet been added to the ORM but is about to be added
                if e in catalogue._added_events:
                    catalogue._added_events.remove(e)

        for e in catalogue._added_events:
            if not hasattr(e, "_backend_entity"):
                self.save_events(e)
            entity.events.append(e._backend_entity)

        self.session.add(entity)

    def save_event(self, event: Event):
        entity = getattr(event, "_backend_entity", None)

        if entity:  # update
            entity.start = event.start
            entity.end = event.end
            entity.author = event.author
            entity.uuid = event.uuid
        else:  # insert
            entity = orm.Event(event.start, event.end, event.author, event.uuid)
            event._backend_entity = entity

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in event.variable_attributes_as_dict().items():
            entity[k] = v

        self.session.add(entity)

    def get_catalogues(self, base: Union[Event, Predicate] = None) -> List[Catalogue]:
        if base:
            if isinstance(base, Predicate):
                raise NotImplemented('Predicate is not yet implemented')
            elif isinstance(base, Event):  # catalogues of an Event
                q = self.session.query(orm.Catalogue).filter(orm.Catalogue.events.any(id=base._backend_entity.id))
            else:
                raise AttributeError('Invalid instance of given base object.')
        else:
            q = self.session.query(orm.Catalogue)

        catalogues = []
        for c in q.all():
            attr = {v.key: v.value for _, v in c.attributes.items()}
            catalogue = Catalogue(c.name, c.author, predicate=pickle.loads(c.predicate), **attr)
            catalogue._backend_entity = c
            catalogues += [catalogue]

        return catalogues

    def get_events(self, base: Union[Catalogue, Predicate] = None) -> List[Event]:
        if base:
            if isinstance(base, Catalogue):
                q = self.session.query(orm.Event).filter(orm.Event.catalogues.any(id=base._backend_entity.id))
            elif isinstance(base, Predicate):
                raise AttributeError('Predicates not yet supported.')
            else:
                raise AttributeError('Invalid instance of given base object.')
        else:
            q = self.session.query(orm.Event)

        events = []
        for e in q:
            attr = {v.key: v.value for _, v in e.attributes.items()}
            event = Event(e.start, e.end, e.author, e.uuid, **attr)
            event._backend_entity = e
            events += [event]

        return events

    def commit(self):
        self.session.commit()


backend = _Backend(url='sqlite:///' + str(Path.joinpath(Path.home(), '.space-event-catalogue.sqlite')))
