from pymongo import WriteConcern
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern


def commit_or_rollback_with_retry(session, rollbacked=False):
    # cannot retry bacause documentDB doesn't support retryable writes
    while True:
        try:
            # Commit uses write concern set at transaction start.
            if not rollbacked:
                session.commit_transaction()
                print("Transaction committed.")
                break
        except (ConnectionFailure, OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                print("UnknownTransactionCommitResult, retrying "
                      "commit operation ...")
                continue
            else:
                print("Error during commit ...")
                raise


class Transaction:
    def __init__(self, mongo_client):
        self.mongo_client = mongo_client
        self.session = self.mongo_client.start_session().__enter__()
        self.rollbacked = False

        self.transaction = self.session.start_transaction(
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern(w="majority"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.transaction.__exit__(exc_type, exc_val, traceback)
        self.session.__exit__(exc_type, exc_val, traceback)
