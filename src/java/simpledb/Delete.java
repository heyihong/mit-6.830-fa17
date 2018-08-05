package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;

    private OpIterator child;

    private int count;

    private boolean hasNext;

    private static final TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.hasNext = false;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    private void executeDelete() throws DbException, TransactionAbortedException {
        child.open();
        try {
            count = 0;
            while (child.hasNext()) {
                Tuple t = child.next();
                DbFile dbFile =
                        Database
                                .getCatalog()
                                .getDatabaseFile(t.getRecordId().getPageId().getTableId());
                try {
                    dbFile.deleteTuple(tid, t);
                } catch (IOException ioe) {
                    throw new DbException(ioe.getMessage());
                }
                count++;
            }
        } finally {
            child.close();
        }
        hasNext = true;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        executeDelete();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        executeDelete();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!hasNext) {
            return null;
        }
        hasNext = false;
        Tuple t = new Tuple(getTupleDesc());
        t.setField(0, new IntField(count));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
