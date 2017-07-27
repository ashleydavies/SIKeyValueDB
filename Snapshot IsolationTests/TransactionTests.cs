using Microsoft.VisualStudio.TestTools.UnitTesting;
using Snapshot_Isolation;

namespace Snapshot_IsolationTests {
    [TestClass]
    public class TransactionTests {
        private readonly Database<int, int> _database = new Database<int, int>(0);
        private readonly Transaction<int, int> _transaction;

        public TransactionTests() {
            _transaction = new Transaction<int, int>(_database);
        }

        [TestMethod]
        public void BeginsAtInitState() {
            Assert.AreEqual(_transaction.Status, TransactionStatus.Init);
        }

        [TestMethod]
        public void CanStartTransaction() {
            _transaction.Start();
            Assert.AreEqual(_transaction.Status, TransactionStatus.InProgress);
        }

        [TestMethod]
        [ExpectedException(typeof(TransactionException))]
        public void CannotStartTransactionTwice() {
            _transaction.Start();
            _transaction.Start();
        }

        [TestMethod]
        public void CanReadNullValueWhenNotWritten() {
            _transaction.Start();
            Assert.AreEqual(_transaction.Read(4), 0);
        }

        [TestMethod]
        public void CanCommitNoUpdates() {
            _transaction.Start();
            Assert.IsTrue(_transaction.TryCommit());
            Assert.AreEqual(_transaction.Status, TransactionStatus.Committed);
        }

        [TestMethod]
        public void CanCommitAnUpdate() {
            _transaction.Start();
            _transaction.Write(1, 10);
            Assert.AreEqual(_transaction.Read(1), 10);
            Assert.IsTrue(_transaction.TryCommit());
            Assert.AreEqual(_transaction.Status, TransactionStatus.Committed);
        }

        [TestMethod]
        public void CannotObserveUpdates() {
            Transaction<int, int> transaction2 = new Transaction<int, int>(_database);

            _transaction.Start();
            transaction2.Start();
            _transaction.Write(4, 2);
            Assert.AreEqual(_transaction.Read(4), 2);
            Assert.AreNotEqual(transaction2.Read(4), 2);
        }

        [TestMethod]
        public void CannotCommitClashingUpdate() {
            Transaction<int, int> transaction2 = new Transaction<int, int>(_database);

            _transaction.Start();
            transaction2.Start();
            _transaction.Write(1, 5);
            transaction2.Write(1, 6);
            Assert.IsTrue(_transaction.TryCommit());
            Assert.IsFalse(transaction2.TryCommit());
            Assert.AreEqual(_transaction.Status, TransactionStatus.Committed);
            Assert.AreEqual(transaction2.Status, TransactionStatus.Aborted);
        }

        [TestMethod]
        public void CanObserveChangeFromPreviousTransaction() {
            _transaction.Start();
            _transaction.Write(3, 5);
            _transaction.TryCommit();

            Transaction<int, int> transaction2 = new Transaction<int, int>(_database);
            transaction2.Start();
            Assert.AreEqual(transaction2.Read(3), 5);
        }

        [TestMethod]
        public void CanAchieveWriteSkew() {
            Transaction<int, int> t1 = new Transaction<int, int>(_database);
            Transaction<int, int> t2 = new Transaction<int, int>(_database);

            _transaction.Start();
            _transaction.Write(1, 100);
            _transaction.Write(2, 100);
            Assert.IsTrue(_transaction.Read(1) + _transaction.Read(2) >= 0);
            _transaction.TryCommit();

            t1.Start();
            t2.Start();

            if (t1.Read(1) + t1.Read(2) - 200 >= 0) {
                t1.Write(1, t1.Read(1) - 200);
            }

            if (t2.Read(1) + t2.Read(2) - 200 >= 0) {
                t2.Write(2, t2.Read(2) - 200);
            }

            Assert.IsTrue(t1.Read(1) + t1.Read(2) >= 0);
            Assert.IsTrue(t2.Read(1) + t2.Read(2) >= 0);

            t1.TryCommit();
            t2.TryCommit();

            Transaction<int, int> t3 = new Transaction<int, int>(_database);
            t3.Start();
            Assert.IsFalse(t3.Read(1) + t3.Read(2) >= 0);
        }
    }
}