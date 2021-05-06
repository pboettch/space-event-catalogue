from catalogue.orm.orm import Event, CatalogueEngine, EventsKeyValue
from sqlalchemy import and_

import datetime as dt

if __name__ == "__main__":
    engine = CatalogueEngine('sqlite:///local.db')
    session = engine.session()

    generate_events = False

    if generate_events:  # generate events
        print("Generating events")
        import datetime as dt
        import random

        for i in range(100000):

            a, b = sorted([random.randint(0, 2 ** 31), random.randint(0, 2 ** 31)])

            start = dt.datetime.fromtimestamp(a)
            end = dt.datetime.fromtimestamp(b)

            event = Event(start, end, 'Patrick')

            meta = {'priority': [1, 2, 3, 4, 5, 6, 7, 8],
                    'mission': ['mms1', 'mms2', 'cluster1', 'cluster2'],
                    'season': ['spring', 'summer', 'autumn', 'winter'],
                    'moon-phase': ['full moon', 'first quarter', 'new moon', 'last quarter'],
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

            session.add(event)
        session.commit()

    if True:
        print("Some queries")


        q = session.query(Event) \
            .filter(Event.with_characteristic('moon-phase', 'full moon'))
        print('moon-phase set to full moon: ', q.count())

        q = session.query(Event) \
            .filter(and_(~Event.with_characteristic('moon-phase', 'full moon'),
                         Event.kv_store.any(EventsKeyValue.key == 'moon-phase')))
        print('moon-phase other that full moon, but moon-phase set: ', q.count())

        q = session.query(Event) \
            .filter(~Event.with_characteristic('moon-phase', 'full moon'))
        print('moon-phase other that full moon, or no moon-phase: ', q.count())

        q = session.query(Event) \
            .filter(and_(~Event.with_characteristic('moon-phase', 'full moon'),
                         Event.kv_store.any(EventsKeyValue.key == 'moon-phase')))
        print('moon-phase other that full moon, or no moon-phase: ', q.count())

        last_month = dt.datetime.now() - dt.timedelta(days=30)

        q = session.query(Event) \
            .filter(Event.kv_store.any(
                and_(EventsKeyValue.key == "creation",
                     EventsKeyValue.value <= dt.datetime.now(),
                     EventsKeyValue.value >= last_month)))

        print('events created during last 30 days:', q.count())


