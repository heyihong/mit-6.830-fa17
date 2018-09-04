package simpledb;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageLockManager implements LockManager<PageId> {

    private final Lock lock;

    private final Map<PageId, LockInfo> pidToLockInfo;

    private final Map<TransactionId, TransactionInfo> tidToTxnInfo;

    private static class TransactionInfo {

        boolean shouldAbort;

        // lockReq != null means that exists a reqQueue contains lockReq
        // lockReq == null means that non reqQueue contains lockReq related to tid
        LockReq lockReq;

        final Set<PageId> lockIds;

        public TransactionInfo() {
            shouldAbort = false;
            lockReq = null;
            lockIds = new HashSet<>();
        }
    }

    private enum LockReqState {
        ACQUIRE_READ,
        ACQUIRE_WRITE,
        ACQUIRED,
        ABORTED
    }

    private static class LockReq {

        final PageId pid;

        final TransactionId tid;

        LockReqState state;

        LockReq(LockReqState state, TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
            this.state = state;
        }
    }

    private class LockInfo {

        LockInfo() {
            this.cond = lock.newCondition();
            this.reqQueue = new LinkedList<>();
            this.isWriteLock = false;
            this.holders = new HashSet<>();
        }

        private final Condition cond;

        // reqQueue should satisfy:
        // 1. Should not contain any aborted req
        // 2. Should not contain duplicate tid
        private final LinkedList<LockReq> reqQueue;

        // If isWriteLock only has meaning when holders.size() > 0
        private boolean isWriteLock;

        private final Set<TransactionId> holders;

        // canMakeProgress returns true if reqQueue could pop head
        private boolean canMakeProgress() {
            if (reqQueue.isEmpty()) {
                return false;
            }
            if (holders.isEmpty()) {
                return true;
            }
            if (!isWriteLock && reqQueue.getFirst().state == LockReqState.ACQUIRE_READ) {
                return true;
            }
            return holders.size() == 1 && holders.contains(reqQueue.getFirst().tid);
        }

        private void makeProgress() {
            while (canMakeProgress()) {
                LockReq head = reqQueue.removeFirst();
                getOrCreateTransactionInfo(head.tid).lockReq = null;
                getOrCreateTransactionInfo(head.tid).lockIds.add(head.pid);

                holders.add(head.tid);
                isWriteLock = head.state == LockReqState.ACQUIRE_WRITE;
                head.state = LockReqState.ACQUIRED;

                cond.signalAll();
            }
        }

        private void abort(TransactionId tid) {
            TransactionInfo txnInfo = tidToTxnInfo.get(tid);
            if (txnInfo == null) {
                throw new IllegalArgumentException();
            }

            txnInfo.shouldAbort = true;
            if (txnInfo.lockReq != null) {
                LockInfo lockInfo = pidToLockInfo.get(txnInfo.lockReq.pid);
                if (lockInfo == null) {
                    throw new IllegalStateException();
                }

                if (!lockInfo.reqQueue.remove(txnInfo.lockReq)) {
                    throw new IllegalStateException();
                }
                txnInfo.lockReq.state = LockReqState.ABORTED;
                txnInfo.lockReq = null;

                lockInfo.cond.signalAll();
            }
        }

        boolean isEmpty() {
            return holders.isEmpty() && reqQueue.isEmpty();
        }

        void handleLockReq(LockReq req) throws TransactionAbortedException {
            if (req.state != LockReqState.ACQUIRE_READ &&
                    req.state != LockReqState.ACQUIRE_WRITE) {
                throw new IllegalArgumentException();
            }

            boolean isHolder = holders.contains(req.tid);

            if (isHolder) {
                if (isWriteLock || req.state == LockReqState.ACQUIRE_READ) {
                    // Have already had required permission
                    return;
                }
            }

            // Wound-wait algorithm
            for (TransactionId tid : holders) {
                if (tid.myid > req.tid.myid &&
                        (isWriteLock ||
                                req.state == LockReqState.ACQUIRE_WRITE)) {
                    abort(tid);
                }
            }
            for (LockReq lr : reqQueue) {
                if (lr.tid.myid > req.tid.myid &&
                        (lr.state == LockReqState.ACQUIRE_WRITE ||
                                req.state == LockReqState.ACQUIRE_WRITE)) {
                    abort(lr.tid);
                }
            }

            if (isHolder) {
                makeProgress();
                // For upgrading lock, holders must be read locks.
                // If reqQueue is not empty after makeProgess(),
                // there is a write lock request whose tid
                // is smaller that current request. Then
                // this transaction should be aborted according
                // to wound-wait algorithm.
                if (!reqQueue.isEmpty()) {
                    throw new IllegalStateException();
                }
            }

            reqQueue.add(req);
            getOrCreateTransactionInfo(req.tid).lockReq = req;

            while (true) {
                makeProgress();

                if (req.state == LockReqState.ACQUIRED) {
                    break;
                }
                if (req.state == LockReqState.ABORTED) {
                    throw new TransactionAbortedException();
                }

                try {
                    cond.await();
                } catch (InterruptedException e) {}
            }

        }

        boolean isHolder(TransactionId tid) {
            return holders.contains(tid);
        }

        void unlock(TransactionId tid) {
            if (!holders.remove(tid)) {
                throw new IllegalArgumentException();
            }
            if (canMakeProgress()) {
                cond.signalAll();
            }
        }
    }

    public PageLockManager() {
        lock = new ReentrantLock();
        pidToLockInfo = new HashMap<>();
        tidToTxnInfo = new HashMap<>();
    }

    private LockInfo getOrCreateLockInfo(PageId pid) {
        LockInfo lockInfo = pidToLockInfo.get(pid);
        if (lockInfo == null) {
            lockInfo = new LockInfo();
            pidToLockInfo.put(pid, lockInfo);
        }
        return lockInfo;
    }

    private void unlockAndRemoveEmptyLockInfo(
            PageId lid,
            TransactionId tid
    ) {
        LockInfo lockInfo = pidToLockInfo.get(lid);
        lockInfo.unlock(tid);
        if (lockInfo.isEmpty()) {
            pidToLockInfo.remove(lid);
        }
    }

    private TransactionInfo getOrCreateTransactionInfo(TransactionId tid) {
        TransactionInfo transactionInfo = tidToTxnInfo.get(tid);
        if (transactionInfo == null) {
            transactionInfo = new TransactionInfo();
            tidToTxnInfo.put(tid, transactionInfo);
        }
        return transactionInfo;
    }

    private void validateTxnInfo(
            TransactionInfo txnInfo
    ) throws TransactionAbortedException {
        if (txnInfo.lockReq != null) {
            throw new IllegalStateException();
        }
        if (txnInfo.shouldAbort) {
            throw new TransactionAbortedException();
        }
    }

    @Override
    public boolean holdsLock(
            TransactionId tid,
            PageId lid
    ) {
        lock.lock();
        try {
            LockInfo lockInfo = pidToLockInfo.get(lid);
            if (lockInfo == null) {
                return false;
            }
            return lockInfo.isHolder(tid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void readLock(
            TransactionId tid,
            PageId lid
    ) throws TransactionAbortedException {
        lock.lock();
        try {
            TransactionInfo txnInfo = getOrCreateTransactionInfo(tid);
            validateTxnInfo(txnInfo);

            LockInfo lockInfo = getOrCreateLockInfo(lid);
            lockInfo.handleLockReq(new LockReq(LockReqState.ACQUIRE_READ, tid, lid));

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void writeLock(
            TransactionId tid,
            PageId lid
    ) throws TransactionAbortedException {
        lock.lock();
        try {
            TransactionInfo txnInfo = getOrCreateTransactionInfo(tid);
            validateTxnInfo(txnInfo);

            LockInfo lockInfo = getOrCreateLockInfo(lid);
            lockInfo.handleLockReq(new LockReq(LockReqState.ACQUIRE_WRITE, tid, lid));

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unlock(
            TransactionId tid,
            PageId lid
    ) {
        lock.lock();
        try {
            TransactionInfo txnInfo = tidToTxnInfo.get(tid);
            if (!txnInfo.lockIds.remove(lid)) {
                throw new IllegalStateException();
            }

            unlockAndRemoveEmptyLockInfo(lid, tid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unlocks(TransactionId tid) {
        lock.lock();
        try {
             TransactionInfo txnInfo = tidToTxnInfo.remove(tid);
             if (txnInfo == null) {
                 return;
             }
             if (txnInfo.lockReq != null) {
                 throw new IllegalStateException();
             }
             for (PageId pid : txnInfo.lockIds) {
                 unlockAndRemoveEmptyLockInfo(pid, tid);
             }
        } finally {
            lock.unlock();
        }
    }
}
