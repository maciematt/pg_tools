# pg_tools

Common tools and methods used with SQLAlchemy/postgres.

## Installation

Install directly from github via pip:

`pip install --upgrade git+https://github.com/pharmhax/pg_tools`

## Usage

Here we're assuming a familiar structure where the script that uses `pg_tools` is in a location with access to db/models.py. Here's more or less what one would typically do with `pg_tools`:

```
import db.models as models
import pg_tools

## Below, pg_tools assumes it'll find all db settings in db.settings.
engine = pg_tools.db_connect() 
models.Base.metadata.bind = engine
DBSession = pg_tools.sessionmaker(bind = engine)
session = DBSession()
```

So, as indicated above, inside models a declarative base needs to be declared and named "Base".

Then optionally you can run something like:

```
pg_tools.create_database()
pg_tools.create_schema('human_disease_transcriptomics')
pg_tools.create_tables(models.Base, engine)
```

that's if you need to create the database, create the schema, and create all the tables from models.

