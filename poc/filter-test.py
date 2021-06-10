from catalogue import get_catalogues, get_events, save, Event, Catalogue
from catalogue.filter import Comparison, All, Field, Attribute, Not, Predicate, Any

import datetime as dt
import datetime

class Filter:
    def __init__(self, condition: Predicate):
        self._condition = condition

    def __repr__(self):
        return self._condition.__repr__()


if __name__ == "__main__":
    condition_all = All(Comparison('>=', Field('start'), dt.datetime.now()),
                        Comparison('<=', Field('end'), dt.datetime.now()))

    f = Filter(condition_all)
    print(f)

    condition_author = Any(Comparison('==', Attribute('author'), "Patrick"),
                           Comparison('==', Attribute('author'), "Alexis"))
    f = Filter(condition_author)
    print(f)

    cond = All(condition_all, condition_author)

    f = Filter(cond)
    print(f)

    r = repr(cond)

    cond2 = eval(r)

    print(r == repr(cond2))



