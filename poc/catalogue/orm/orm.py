from sqlalchemy import Column, Integer, DateTime, ForeignKey, Unicode, UnicodeText, \
    create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.associationproxy import association_proxy

import json

Base = declarative_base()


class CatalogueEngine:
    def __init__(self, url: str):
        self.engine = create_engine(url)
        self.session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)


class ProxiedDictMixin(object):
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.

    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def __repr__(self):
        return json.dumps({k: v for k, v in self.proxied.items() if not k.startswith('_')}, indent=1)


class EventsKeyValue(Base):
    """Meta-data (key-value-store) for an event."""

    __tablename__ = "events_metadata"

    event_id = Column(ForeignKey("events.id"), primary_key=True)
    key = Column(Unicode(64), primary_key=True)
    value = Column(UnicodeText)


class Event(ProxiedDictMixin, Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True)

    start = Column(DateTime)
    end = Column(DateTime)
    author = Column(UnicodeText)

    kv_store = relationship(
        "EventsKeyValue", collection_class=attribute_mapped_collection("key")
    )

    _proxied = association_proxy(
        "kv_store",
        "value",
        creator=lambda key, value: EventsKeyValue(key=key, value=value),
    )

    def __init__(self, start, end, author):
        self.start = start
        self.end = end
        self.author = author

    def __repr__(self):
        # return
        return f'Event({self.start}, {self.end}, {self.author}), meta=' + self._proxied.__repr__()

    @classmethod
    def with_characteristic(cls, key, value):
        return cls.kv_store.any(key=key, value=value)
