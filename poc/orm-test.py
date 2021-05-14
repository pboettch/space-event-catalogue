from catalogue.orm.orm import Event, Backend, EventAttributes, Catalogue
from sqlalchemy import and_

from pathlib import Path

import datetime as dt

from uuid import uuid4

if __name__ == "__main__":
    # engine = CatalogueEngine('sqlite:///local.db')
    engine = Backend('sqlite:///' + str(Path.joinpath(Path.home(), '.space-event-catalogue.sqlite')))
    session = engine.session()

    generate_events = True
    # generate_events = False

    if generate_events:  # generate events
        print("Generating events")
        import random

        cat = Catalogue('Greatest Catalogue', 'Patrick')
        cat['attr1'] = 1
        cat['attr2'] = "str"
        cat['attr3'] = dt.datetime.now()
        session.add(cat)

        cat2 = Catalogue('Great Catalogue', 'Patrick')
        cat2['attr1'] = 10
        cat2['attr2'] = "Hello"
        session.add(cat2)

        for i in range(10):

            a, b = sorted([random.randint(0, 2 ** 31), random.randint(0, 2 ** 31)])

            start = dt.datetime.fromtimestamp(a)
            end = dt.datetime.fromtimestamp(b)

            event = Event(start, end, 'Patrick', str(uuid4()))

            meta = {'priority': [1, 2, 3, 4, 5, 6, 7, 8],
                    'mission': ['mms1', 'mms2', 'cluster1', 'cluster2'],
                    'season': ['spring', 'summer', 'autumn', 'winter'],
                    'moon_phase': ['full moon', 'first quarter', 'new moon', 'last quarter'],
                    'creation': [dt.datetime.fromtimestamp(random.randint(0, 2 ** 31)),
                                 dt.datetime.fromtimestamp(random.randint(0, 2 ** 31))]}

            # generate random number of metas with a random value
            meta_count = random.randint(1, len(meta))

            for _ in range(meta_count):
                while True:
                    key = random.choice(list(meta.keys()))
                    if key not in event:
                        event[key] = random.choice(meta[key])
                        break

            cat.events.append(event)
            cat2.events.append(event)

            session.add(event)

        session.commit()

    if True:
        print("Some queries")

        q = session.query(Event) \
            .filter(Event.with_characteristic('moon-phase', 'full moon'))
        print('moon-phase set to full moon: ', q.count())

        q = session.query(Event) \
            .filter(and_(~Event.with_characteristic('moon-phase', 'full moon'),
                         Event.attributes.any(EventAttributes.key == 'moon-phase')))
        print('moon-phase other that full moon, but moon-phase set: ', q.count())

        q = session.query(Event) \
            .filter(~Event.with_characteristic('moon-phase', 'full moon'))
        print('moon-phase other that full moon, or no moon-phase: ', q.count())

        q = session.query(Event) \
            .filter(and_(~Event.with_characteristic('moon-phase', 'full moon'),
                         Event.attributes.any(EventAttributes.key == 'moon-phase')))
        print('moon-phase other that full moon, or no moon-phase: ', q.count())

        last_month = dt.datetime.now() - dt.timedelta(days=30)

        q = session.query(Event) \
            .filter(Event.attributes.any(
            and_(EventAttributes.key == "creation",
                 EventAttributes.value <= dt.datetime.now(),
                 EventAttributes.value >= last_month)))

        print('events created during last 30 days:', q.count())
