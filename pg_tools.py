#!/usr/bin/env python3




import psycopg2
# import ego.io
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from base64 import b64decode
from io import StringIO
import numpy


import db.settings as settings




"""
For now this is a set of tools with a very simple structure; all that needs to be done
is that settings need to be located in such a location that the import above will work.
Then, one can simply import pg_tools wherever they may need, and create a session via:

```
  engine = pg_tools.db_connect()
  Base = pg_tools.Base.
  Base.metadata.bind = engine
  DBSession = pg_tools.sessionmaker(bind = engine)
  session = DBSession()
```

Then you import models, and Base should be visible to all table classes, so ... hold on
New idea - just import pg_tools in models via `from pg_tools import *`. Then in the main
script just import models as before, and do everything like before. I'll have to check
this out.

"""



settings.DATABASE['password'] = b64decode(settings.DATABASE['password']).decode('utf-8')
Base = declarative_base()




def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))



def create_database(db_name, new_db_name):
    db_settings = settings.DATABASE.copy()
    db_settings['database'] = db_name # this is where you connect to set up, e.g. postgres
    engine = create_engine(URL(**db_settings))
    conn = engine.connect()
    conn.execute("commit")
    conn.execute("create database %s" % new_db_name)
    conn.close()



def create_schema(schema_name):
    db_settings = settings.DATABASE.copy()
    engine = create_engine(URL(**db_settings))
    conn = engine.connect()
    conn.execute("commit")
    conn.execute("create schema %s" % schema_name)
    conn.close()



def create_tables(engine):
    Base.metadata.create_all(engine)



def insert_new_row(items_to_add, table, session):
    new_item = table(**items_to_add)
    session.add(new_item)
    session.commit()



def bulk_insert_rows(rows, table, kwargs = None):

    """
    This version is faster than the alt below by a SHOCKING margin - like probably 2 orders of magnitude.
    Make sure that no funky characters are saved in file. Also NAs have to be sent in as '' - empty strings.
    Note - apparently this NA / NaN issue didn't actually do anything when I had NaN's in the dataframe.
    """

    if not kwargs:
        kwargs = {}

    with psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" % (settings.DATABASE['host'], settings.DATABASE['port'], settings.DATABASE['database'], settings.DATABASE['username'], settings.DATABASE['password'])) as conn:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'latin1'")
            data_io = StringIO()
            rows.to_csv(data_io, index = False, header = False, sep = '\t', **kwargs)#, float_format = '%.f')
            data_io.seek(0)
            cur.copy_from(data_io, table, null = '')



def bulk_insert_rows_alt(items_to_add, table, session):
    """
    Note: Panda's NaNs can't be passed here. They have to be subbed with postgres NULLs via
    psycopg2.extensions.AsIs('NULL') - however, in case of numbers this is more convoluted, e.g.:
    new_known_drugs['product'] = new_known_drugs['product'].where(pd.notnull(new_known_drugs['product']), None)
    """
    row_list = items_to_add.to_dict('records')
    session.add_all([
        table(**row) for row in row_list
    ])
    session.commit()




def update_multiple_rows(df_of_updates, table, session):
    """
    Note: Panda's NaNs can't be passed here. They have to be subbed with postgres NULLs via
    psycopg2.extensions.AsIs('NULL')
    """
    row_list = df_of_updates.to_dict('records')
    for row in row_list:
        session.merge(table(**row))

    session.commit()




def adapt_numpy_int64(numpy_int64):
    """ Adapting numpy.int64 type to SQL-conform int type using psycopg extension, see [1]_ for more info.
    This is an official part of ego.io, so once I can install eGo it should be ok to get rid of this.
    References
    ----------
    .. [1] http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax
    """
    return AsIs(numpy_int64)

register_adapter(numpy.int64, adapt_numpy_int64)



