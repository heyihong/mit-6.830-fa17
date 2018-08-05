package simpledb;

import com.sun.corba.se.impl.orb.DataCollectorBase;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;

    private OpIterator child;

    private final DbFile dbFile;

    private int count;

    private boolean hasNext;

    private static final TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
        if (!this.dbFile
                .getTupleDesc()
                .equals(child.getTupleDesc())) {
            throw new DbException("tuple desc of child differs from table");
        }
        this.hasNext = false;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    private void executeInsert() throws DbException, TransactionAbortedException {
        child.open();
        try {
            count = 0;
            while (child.hasNext()) {
                Tuple t = child.next();
                try {
                    dbFile.insertTuple(tid, t);
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

    public void open() throws DbException, TransactionAbortedException  {
        super.open();
        executeInsert();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        executeInsert();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
