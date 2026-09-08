import os
import logging
from dotenv import load_dotenv
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DbManager(object):

    engine = None
    dotenv_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '../../.env'
        )
    )
    load_dotenv(dotenv_path=dotenv_path)

    def __init__(self):
        self.server = os.environ["DB_HOST"]
        self.database = os.environ["DB_NAME"]
        self.username = os.environ["DB_USER"]
        self.password = os.environ["DB_PASSWORD"]
        self.driver = os.environ["DB_DRIVER"]
        self.port = os.environ["DB_PORT"]
        self.db_url = f"{self.driver}://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}"
        self.engine = self.get_db_connection()

    def get_db_connection(self):
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

        engine = create_engine(self.db_url)
        return engine.connect()

    def get_engine(self):
        """Returns a new SQLAlchemy engine"""
        return create_engine(self.db_url)

    def get_db(self):
        session = sessionmaker(
            bind=create_engine(
                self.db_url
            )
        )()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


    @contextmanager
    def get_orm_session(self):
        db_gen = self.get_db()
        session = next(db_gen)
        try:
            yield session
            try:
                next(db_gen)
            except StopIteration:
                pass
        except Exception:
            try:
                db_gen.throw(*__import__("sys").exc_info())
            except StopIteration:
                pass
            raise
