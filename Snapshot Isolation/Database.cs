using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Snapshot_Isolation {
    public class Database<TKey, TValue> {
        private readonly Dictionary<TKey, TValue> _contents;

        private readonly Dictionary<Transaction<TKey, TValue>, Dictionary<TKey, TValue>>
            _openTransactionsCachedValueDictionary;

        private readonly TValue _nullValue;

        public Database(TValue nullValue) {
            _contents = new Dictionary<TKey, TValue>();
            _openTransactionsCachedValueDictionary =
                new Dictionary<Transaction<TKey, TValue>, Dictionary<TKey, TValue>>();
            _nullValue = nullValue;
        }

        public void StartTransaction(Transaction<TKey, TValue> transaction) {
            _openTransactionsCachedValueDictionary[transaction] = new Dictionary<TKey, TValue>();
        }

        public TValue Read(Transaction<TKey, TValue> transaction, TKey key) {
            if (_openTransactionsCachedValueDictionary.ContainsKey(transaction) &&
                _openTransactionsCachedValueDictionary[transaction].ContainsKey(key)) {
                return _openTransactionsCachedValueDictionary[transaction][key];
            }

            return _contents.ContainsKey(key) ? _contents[key] : _nullValue;
        }

        public bool TryCommit(Transaction<TKey, TValue> transaction, Dictionary<TKey, TValue> updates) {
            // Don't allow a transaction to commit if it has been forged or contains a conflicting update
            if (!transaction.Updated(updates) ||
                !_openTransactionsCachedValueDictionary.ContainsKey(transaction) ||
                updates.Keys.Intersect(_openTransactionsCachedValueDictionary[transaction].Keys).Any()) {
                return false;
            }

            // Remove the transaction's cached values
            _openTransactionsCachedValueDictionary.Remove(transaction);

            // Approve the commit
            foreach (var update in updates) {
                foreach (var openTransaction in _openTransactionsCachedValueDictionary.Values) {
                    if (!openTransaction.ContainsKey(update.Key)) {
                        openTransaction[update.Key] = _contents.ContainsKey(update.Key)
                            ? _contents[update.Key]
                            : _nullValue;
                    }
                }

                _contents[update.Key] = update.Value;
            }

            return true;
        }
    }
}
