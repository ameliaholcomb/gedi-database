from gedidb.database.gedidb_schema import Base
import sqlalchemy


from gedidb import environment


def get_engine():
    return sqlalchemy.create_engine(environment.DB_CONFIG, echo=False)

def get_test_engine():
    return sqlalchemy.create_engine(environment.DB_TEST_CONFIG, echo=False)

def maybe_create_tables():
    with get_engine().begin() as conn:
        Base.metadata.create_all(conn)
    return
