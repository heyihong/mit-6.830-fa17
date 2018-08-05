package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final Type gbfieldtype;

    private final int afield;

    private final Op what;

    private final Map<Field, Integer[]> aggMap;

    private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggMap = new HashMap<>();
        Type[] types = gbfield == NO_GROUPING ?
                new Type[]{Type.INT_TYPE} :
                new Type[]{gbfieldtype, Type.INT_TYPE};
        this.tupleDesc = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Integer val = ((IntField)tup.getField(afield)).getValue();
        Integer[] aggVals = aggMap.get(key);
        if (aggVals == null) {
            switch (what) {
                case MIN: case MAX: case SUM:
                    aggMap.put(key, new Integer[]{val});
                    break;
                case AVG:
                    aggMap.put(key, new Integer[]{val, 1});
                    break;
                case COUNT:
                    aggMap.put(key, new Integer[]{1});
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (what) {
                case MIN:
                    aggVals[0] = Math.min(aggVals[0], val);
                    break;
                case MAX:
                    aggVals[0] = Math.max(aggVals[0], val);
                    break;
                case SUM:
                    aggVals[0] += val;
                    break;
                case AVG:
                    aggVals[0] += val;
                    aggVals[1] += 1;
                    break;
                case COUNT:
                    aggVals[0] += 1;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    private Integer calculateAggValue(Integer[] vals) {
        switch (what) {
            case MIN: case MAX: case SUM: case COUNT:
                return vals[0];
            case AVG:
                return vals[0] / vals[1];
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {

            Iterator<Map.Entry<Field, Integer[]>> iter = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                iter = aggMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iter == null) {
                    throw new IllegalStateException();
                }
                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map.Entry<Field, Integer[]> entry = iter.next();
                Tuple t = new Tuple(getTupleDesc());
                IntField f = new IntField(calculateAggValue(entry.getValue()));
                if (gbfield == NO_GROUPING) {
                    t.setField(0, f);
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1, f);
                }
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (iter == null) {
                    throw new IllegalStateException();
                }
                iter = aggMap.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                iter = null;
            }
        };
    }

}
