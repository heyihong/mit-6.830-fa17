package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final Type gbfieldtype;

    private final int afield;

    private final Op what;

    private final Map<Field, Integer> countMap;

    private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
           throw new IllegalArgumentException("what != COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.countMap = new HashMap<>();
        Type[] types = gbfield == NO_GROUPING ?
                new Type[]{Type.INT_TYPE} :
                new Type[]{gbfieldtype, Type.INT_TYPE};
        this.tupleDesc = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        switch (what) {
            case COUNT:
                Integer count = countMap.get(key);
                if (count == null) {
                    count = 0;
                }
                countMap.put(key, count + 1);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {

            private Iterator<Map.Entry<Field, Integer>> countIter = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                countIter = countMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (countIter == null) {
                    throw new IllegalStateException();
                }
                return countIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map.Entry<Field, Integer> entry = countIter.next();
                Field val = new IntField(entry.getValue());
                Tuple t = new Tuple(getTupleDesc());
                if (gbfield == NO_GROUPING) {
                    t.setField(0, val);
                    return t;
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1, val);
                    return t;
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (countIter == null) {
                    throw new IllegalStateException();
                }
                countIter = countMap.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                countIter = null;
            }
        };
    }

}
