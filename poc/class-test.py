from catalogue import Event, Catalogue
from catalogue.api import get_catalogues, get_events, save
from catalogue.filter import Comparison, Field, Attribute, All, Not, Has, Match

import random
import datetime as dt

from typing import List


def get_start_end():
    return sorted([dt.datetime.fromtimestamp(random.randint(0, 2 ** 31)),
                   dt.datetime.fromtimestamp(random.randint(0, 2 ** 31))])


def show_events(events: List[Event], info: str = None, count=10):
    print("####", len(events), 'entries, displaying', count, ', ' + info if info is not None else '')
    i = 1
    for e in events:
        if i >= count:
            break
        print(i, e)
        i += 1


if __name__ == "__main__":

    print("# 1 create event collection")
    if 0:
        events = []
        for i in range(100):  # generate events

            a, b = sorted([random.randint(0, 2 ** 31), random.randint(0, 2 ** 31)])

            start = dt.datetime.fromtimestamp(a)
            end = dt.datetime.fromtimestamp(b)

            attr_source = {'priority': [1, 2, 3, 4, 5, 6, 7, 8],
                           'mission': ['mms1', 'mms2', 'cluster1', 'cluster2'],
                           'season': ['spring', 'summer', 'autumn', 'winter'],
                           'moon_phase': ['full moon', 'first quarter', 'new moon', 'last quarter'],
                           'creation': [dt.datetime.fromtimestamp(random.randint(0, 2 ** 31)),
                                        dt.datetime.fromtimestamp(random.randint(0, 2 ** 31))]}

            # generate random number of attributes with a random value
            attr_count = random.randint(1, len(attr_source))

            attrs = {}
            for _ in range(attr_count):
                while True:
                    key = random.choice(list(attr_source.keys()))
                    if key not in attrs:
                        attrs[key] = random.choice(attr_source[key])
                        break

            event = Event(start, end, 'Patrick', **attrs)
            events += [event]

        save(events)

    print("# 2 batch edit of event list")
    if 0:
        print('# get all event and work on the first 5')
        events = get_events()[:5]

        for e in events:
            print('event:', e.start)
            e.start += dt.timedelta(hours=2)

        save(events)

        print('# get all event and print the first 5')
        for e in get_events()[:5]:
            print('event:', e.start)

    print('# 3 add event(s) to new catalogue')
    if 0:
        catalogue = Catalogue("New Catalogue", "Patrick",
                              notes="Catalogue containing the first 10 events",
                              version=1)

        events = get_events()[:10]
        catalogue.add_events(events)
        save(catalogue)

        catalogue = get_catalogues()[0]
        events = get_events(catalogue)
        for e in events:
            print('event:', e.start)

    print('# 4 remove event(s) from catalogue')
    if 0:
        catalogue = get_catalogues()[0]

        events = get_events(catalogue)

        for e in events[::2]:
            print('removing:', e.uuid)
            catalogue.remove_events(e)

        save(catalogue)

        catalogue = get_catalogues()[0]
        events = get_events(catalogue)
        for e in events:
            print('event:', e.start)

    print("# 5 use predicates - create SmartCatalogue")
    if 0:
        catalogue = Catalogue('SmartCatalogue Author=Patrick', 'Patrick',
                              predicate=Comparison('==', Field('author'), 'Patrick'))
        save(catalogue)

    print("# 5 use predicates - use to filter events")
    if 1:
        catalogue = get_catalogues()[-1]
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=10)

        catalogue = Catalogue('SmartCatalogue Author=Patrick', 'Patrick',
                              predicate=Comparison('>', Field('start'), dt.datetime.now()))
        show_events(get_events(catalogue))

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Comparison('==', Attribute('mission'), 'mms2'))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=0)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Comparison('!=', Attribute('mission'), 'mms2'))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=0)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Not(Comparison('==', Attribute('mission'), 'mms2')))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=0)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Has(Attribute('moon_phase')))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=10)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Match(Attribute('moon_phase'), r'oon'))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=10)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=Match(Field('author'), r'k$'))
        show_events(get_events(catalogue), f'{catalogue.predicate}', count=10)

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=All(Comparison('==', Attribute('mission'), 'mms2'),
                                            Comparison('>', Field('start'), dt.datetime.now())))

        show_events(get_events(catalogue), f'{catalogue.predicate}')

        catalogue = Catalogue('SmartCatalogue', 'Patrick',
                              predicate=All(Comparison('==', Attribute('mission'), 'mms2'),
                                            Comparison('<=', Field('start'), dt.datetime.now())))
        show_events(get_events(catalogue), f'{catalogue.predicate}')

    exit(1)
