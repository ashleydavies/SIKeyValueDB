using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snapshot_Isolation {
    public class Transaction<TKey, TValue> {
        public TransactionStatus Status { get; private set; }
        private readonly Database<TKey, TValue> _database;
        private readonly Dictionary<TKey, TValue> _updates;

        public Transaction(Database<TKey, TValue> database) {
            _database = database;
            Status = TransactionStatus.Init;
            _updates = new Dictionary<TKey, TValue>();
        }

        public void Start() {
            if (Status != TransactionStatus.Init)
                throw new TransactionException("Attempt to start an already-started transaction");
            _database.StartTransaction(this);
            Status = TransactionStatus.InProgress;
        }

        public TValue Read(TKey key) {
            if (Status != TransactionStatus.InProgress)
                throw new TransactionException("Cannot read from a transaction if it is not in progress");
            if (_updates.ContainsKey(key)) return _updates[key];
            return _database.Read(this, key);
        }

        public void Write(TKey key, TValue value) {
            if (Status != TransactionStatus.InProgress)
                throw new TransactionException("Cannot write to a transaction if it is not in progress");
            _updates[key] = value;
        }

        public bool TryCommit() {
            if (Status != TransactionStatus.InProgress)
                throw new TransactionException("Cannot commit a transaction if it is not in progress");
            if (_database.TryCommit(this, _updates)) {
                Status = TransactionStatus.Committed;
                return true;
            }

            Status = TransactionStatus.Aborted;
            return false;
        }

        // Allows a database to ascertain if a call to TryCommit was made by a transaction or not.
        // Done by verifying that the updates dictionary provided is the same one the transaction holds.
        public bool Updated(Dictionary<TKey, TValue> updates) {
            return ReferenceEquals(updates, _updates);
        }
    }

    public enum TransactionStatus {
        Init,
        InProgress,
        Committed,
        Aborted
    }
}