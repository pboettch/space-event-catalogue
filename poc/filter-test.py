from catalogue import Event, Catalogue
from catalogue.api import get_catalogues, get_events
from catalogue.filter import Comparison, All, Field, Attribute, Not, Predicate, Any

import datetime as dt
import datetime
import pickle


if __name__ == "__main__":
    condition_all = All(Comparison('>=', Field('start'), dt.datetime.now()),
                        Comparison('<=', Field('end'), dt.datetime.now()))

    print(condition_all)

    condition_author = Any(Comparison('==', Attribute('author'), "Patrick"),
                           Comparison('==', Attribute('author'), "Alexis"))
    print(condition_author)

    cond = Not(All(condition_all, condition_author))

    pickled = pickle.dumps(cond, protocol=3)

    cond2 = pickle.loads(pickled)

    print(repr(cond) == repr(cond2))

    events = get_events(condition_author)


