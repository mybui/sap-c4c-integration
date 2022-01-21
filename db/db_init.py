from sqlalchemy import create_engine

from ..settings import DB_NAME


def db_init(db_name):
    with create_engine(url="postgresql:///",
                       echo="debug",
                       isolation_level="AUTOCOMMIT").connect() as conn:
        conn.execute("CREATE DATABASE {0}".format(db_name))


if __name__ == "__main__":
    db_init(db_name=DB_NAME)
