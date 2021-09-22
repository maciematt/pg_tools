# pg_tools

Common tools and methods used with SQLAlchemy/postgres.

## Installation

Install directly from github via pip:

`pip install git+https://github.com/pharmhax/pg_tools`

## Usage

In your models script import `pg_tools` in models via `from pg_tools import *`. Then in the main script just import models and do everything as before, i.e. make sure to run (in this case the assumption is that models.py is in `./db/`):

```
import db.models as models

engine = models.db_connect()
models.Base.metadata.bind = engine
DBSession = models.sessionmaker(bind = engine)
session = DBSession()
```

Then optionally you can run something like:

```
models.create_database()
models.create_schema('human_disease_transcriptomics')
models.create_tables(engine)
```

that's if you need to create the database, create the schema, and create all the tables from models.

