package simpledb;

/**
 * LockManager allows transaction to acquire read lock, write lock or upgrade lock.
 * It should detect deadlock happen and abort transaction if necessary.
 * Should be thread-safe
 * @param <LockId>
 */
public interface LockManager<LockId> {

    boolean holdsLock(TransactionId tid, LockId lid);

    void readLock(TransactionId tid, LockId lid) throws TransactionAbortedException;

    void writeLock(TransactionId tid, LockId lid) throws TransactionAbortedException;

    void unlock(TransactionId tid, LockId lid);

    void unlocks(TransactionId tid);
}
