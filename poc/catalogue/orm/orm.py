from sqlalchemy import Column, Integer, DateTime, ForeignKey, Unicode, UnicodeText, Boolean, String
from sqlalchemy import create_engine, event, literal_column, case, cast, null
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property

import datetime as dt

import json


Base = declarative_base()


class CatalogueEngine:
    def __init__(self, url: str):
        self.engine = create_engine(url, echo=True)
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


class PolymorphicVerticalProperty(object):
    """A key/value pair with polymorphic value storage.

    The class which is mapped should indicate typing information
    within the "info" dictionary of mapped Column objects; see
    the AnimalFact mapping below for an example.

    """

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    @hybrid_property
    def value(self):
        fieldname, discriminator = self.type_map[self.type]
        if fieldname is None:
            return None
        else:
            return getattr(self, fieldname)

    @value.setter
    def value(self, value):
        py_type = type(value)
        fieldname, discriminator = self.type_map[py_type]

        self.type = discriminator
        if fieldname is not None:
            setattr(self, fieldname, value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        def __init__(self, cls):
            self.cls = cls

        def _fieldname(self, py_type):
            return self.cls.type_map[py_type][0]

        # TODO, see whether the type-name from type_map should be used for and and_-condition
        # TODO, check whether we need to cast?!

        def __eq__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) == other

        def __ne__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) != other

        def __lt__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) < other

        def __gt__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) > other

        def __le__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) <= other

        def __ge__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) >= other

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.key}={self.value}>"


@event.listens_for(
    PolymorphicVerticalProperty, "mapper_configured", propagate=True
)
def on_new_class(mapper, cls_):
    """Look for Column objects with type info in them, and work up
    a lookup table."""

    info_dict = {
        type(None): (None, "none"),
        "none": (None, "none")
    }

    for k, col in mapper.c.items():
        if "type" in col.info:
            python_type, discriminator = col.info["type"]
            info_dict[python_type] = \
                info_dict[discriminator] = (k, discriminator)
    cls_.type_map = info_dict


class EventsKeyValue(PolymorphicVerticalProperty, Base):
    """Meta-data (key-value-store) for an event."""

    __tablename__ = "events_metadata"

    event_id = Column(ForeignKey("events.id"), primary_key=True)
    key = Column(Unicode(64), primary_key=True)
    type = Column(Unicode(16))

    int_value = Column(Integer, info={"type": (int, "integer")})
    char_value = Column(UnicodeText, info={"type": (str, "string")})
    boolean_value = Column(Boolean, info={"type": (bool, "boolean")})
    datetime_value = Column(DateTime, info={"type": (dt.datetime, "datetime")})
    float_value = Column(DateTime, info={"type": (float, "float")})


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
