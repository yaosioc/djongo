# THIS FILE WAS CHANGED ON - 05 Sep 2022

from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern


class Transaction:
    def __init__(self, mongo_client):
        # do initial steps for transaction as noted in mongo documentation for database version 4.0
        self.mongo_client = mongo_client
        self.session = self.mongo_client.start_session().__enter__()
        self.rollbacked = False

        self.transaction = self.session.start_transaction(
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern(w="majority"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        # cannot retry bacause documentDB doesn't support retryable writes
        # exit transaction decorators as noted in mongo documentation for database version 4.0
        self.transaction.__exit__(exc_type, exc_val, traceback)
        self.session.__exit__(exc_type, exc_val, traceback)
