package simpledb;

import sun.plugin.dom.exception.InvalidStateException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final Object[] histObjects;

    private final TupleDesc td;

    private final int ioCostPerPage;

    private final int numPages;

    private final int numTuples;

    private static List<Integer> getIntTypeIdxs(TupleDesc td) {
        List<Integer> idxs = new ArrayList<>();
        for (int i = 0; i != td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                idxs.add(i);
            }
        }
        return idxs;
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);

        TransactionId tid = new TransactionId();
        td = dbFile.getTupleDesc();

        List<Integer> intTypeIdxs = getIntTypeIdxs(td);
        int[] minVal = new int[td.numFields()];
        int[] maxVal = new int[td.numFields()];
        if (!intTypeIdxs.isEmpty()) {
            // Need to do the first phase scan to figure out min and max value
            DbFileIterator iter = dbFile.iterator(tid);
            try {
                iter.open();
                boolean firstTuple = true;
                while (iter.hasNext()) {
                    Tuple t = iter.next();
                    for (int i = 0; i != intTypeIdxs.size(); i++) {
                        int idx = intTypeIdxs.get(i);
                        int value = ((IntField)t.getField(idx)).getValue();
                        if (firstTuple) {
                            minVal[idx] = maxVal[idx] = value;
                        } else {
                            minVal[idx] = Math.min(minVal[idx], value);
                            maxVal[idx] = Math.max(maxVal[idx], value);
                        }
                    }
                    firstTuple = false;
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                iter.close();
            }
        }

        histObjects = new Object[td.numFields()];
        for (int i = 0; i != td.numFields(); i++) {
            switch (td.getFieldType(i)) {
                case INT_TYPE:
                    histObjects[i] = new IntHistogram(NUM_HIST_BINS, minVal[i], maxVal[i]);
                    break;
                case STRING_TYPE:
                    histObjects[i] = new StringHistogram(NUM_HIST_BINS);
                    break;
                default:
                    throw new IllegalStateException("unknown type: " + td.getFieldType(i));
            }
        }
        DbFileIterator iter = dbFile.iterator(tid);
        int numTuples = 0;
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple t = iter.next();
                numTuples++;
                for (int i = 0; i != td.numFields(); i++) {
                    switch (td.getFieldType(i)) {
                        case INT_TYPE:
                            ((IntHistogram)histObjects[i]).addValue(((IntField)t.getField(i)).getValue());
                            break;
                        case STRING_TYPE:
                            ((StringHistogram)histObjects[i]).addValue(((StringField)t.getField(i)).getValue());
                            break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            iter.close();
        }

        this.ioCostPerPage = ioCostPerPage;
        this.numPages = ((HeapFile)dbFile).numPages();
        this.numTuples = numTuples;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        switch (td.getFieldType(field)) {
            case INT_TYPE:
                IntHistogram intHist = (IntHistogram)histObjects[field];
                return intHist.estimateSelectivity(op, ((IntField)constant).getValue());
            case STRING_TYPE:
                StringHistogram stringHist = (StringHistogram)histObjects[field];
                return stringHist.estimateSelectivity(op, ((StringField)constant).getValue());
        }
        throw new IllegalStateException();
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
