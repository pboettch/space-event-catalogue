from . import orm

from .. import Event, Catalogue
from ..filter import Predicate, Comparison, Field, Attribute, All, Any, Match, Has, Not

from pathlib import Path

import pickle
import datetime as dt

from typing import Union, List

from sqlalchemy import create_engine, and_, or_, not_
from sqlalchemy.orm import Session


class PredicateVisitor:
    def __init__(self, orm_class: Union[orm.Event, orm.Catalogue]):
        self._orm_class = orm_class

    def _visit_literal(self, operand: Union[str, int, bool, float, dt.datetime]):
        if type(operand) not in [str, int, bool, float, dt.datetime]:
            raise TypeError("Literal must be of allowed type.")
        return operand

    def _visit_comparison(self, comp: Comparison):
        if not type(comp._lhs) in [Field, Attribute]:
            raise AttributeError('Invalid LHS operand instance.')

        rhs = self._visit_literal(comp._rhs)

        if isinstance(comp._lhs, Field):
            lhs = getattr(self._orm_class, comp._lhs.value)

            if comp._op == '==':
                return lhs == rhs
            elif comp._op == '!=':
                return lhs != rhs
            elif comp._op == '<':
                return lhs < rhs
            elif comp._op == '<=':
                return lhs <= rhs
            elif comp._op == '>':
                return lhs > rhs
            elif comp._op == '>=':
                return lhs >= rhs
            else:
                raise AttributeError('Invalid comparison operator.')

        elif isinstance(comp._lhs, Attribute):
            if comp._op == '==':
                value_comp = self._orm_class._attribute_class.value == rhs
            elif comp._op == '!=':
                value_comp = self._orm_class._attribute_class.value != rhs
            elif comp._op == '<':
                value_comp = self._orm_class._attribute_class.value < rhs
            elif comp._op == '<=':
                value_comp = self._orm_class._attribute_class.value <= rhs
            elif comp._op == '>':
                value_comp = self._orm_class._attribute_class.value > rhs
            elif comp._op == '>=':
                value_comp = self._orm_class._attribute_class.value >= rhs
            else:
                raise AttributeError('Invalid comparison operator.')

            return self._orm_class.attributes.any(
                and_(self._orm_class._attribute_class.key == comp._lhs.value,
                     value_comp))

    def _visit_all(self, all_: All):
        return and_(self.visit_predicate(pred) for pred in all_._predicates)

    def _visit_any(self, any_: Any):
        return or_(self.visit_predicate(pred) for pred in any_._predicates)

    def _visit_not(self, not__: Not):
        return not_(self.visit_predicate(not__._operand))

    def _visit_has(self, has_: Has):
        return self._orm_class.attributes.any(
            and_(self._orm_class._attribute_class.key == has_._operand.value,
                 self._orm_class._attribute_class.value is not None))

    def _visit_match(self, match_: Match):
        if not type(match_._lhs) in [Field, Attribute]:
            raise AttributeError('Invalid LHS operand instance - expected Field or Attribute.')

        if not type(match_._rhs) == str:
            raise AttributeError('Invalid RHS operand instance - expected str.')

        if isinstance(match_._lhs, Field):
            lhs = getattr(self._orm_class, match_._lhs.value)
            return lhs.regexp_match(match_._rhs)

        elif isinstance(match_._lhs, Attribute):
            print(self._orm_class._attribute_class.key)
            print(self._orm_class._attribute_class.value)
            return self._orm_class.attributes.any(
                and_(self._orm_class._attribute_class.key == match_._lhs.value,
                     self._orm_class._attribute_class.value.regexp_match(match_._rhs)))

    def visit_predicate(self, pred: Predicate):
        if isinstance(pred, Comparison):
            return self._visit_comparison(pred)
        elif isinstance(pred, All):
            return self._visit_all(pred)
        elif isinstance(pred, Any):
            return self._visit_any(pred)
        elif isinstance(pred, Not):
            return self._visit_not(pred)
        elif isinstance(pred, Has):
            return self._visit_has(pred)
        elif isinstance(pred, Match):
            return self._visit_match(pred)
        else:
            raise NotImplemented('Unexpected predicated instance.')


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
                self.save_event(e)
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

    def get_events(self, base: Catalogue = None) -> List[Event]:
        if base:
            if isinstance(base, Catalogue):
                if base.predicate:  # "smart catalogue"
                    f = PredicateVisitor(orm.Event).visit_predicate(base.predicate)
                    q = self.session.query(orm.Event).filter(f)
                else:
                    q = self.session.query(orm.Event).filter(orm.Event.catalogues.any(id=base._backend_entity.id))
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
