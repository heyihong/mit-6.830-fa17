package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;

    private final int min;

    private final int max;

    private int total;

    private final int[] counts;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.counts = new int[buckets];
        this.total = 0;
    }

    /**
     * For bucket k in [0, n - 1], it contains elements in
     * [min + k * d / n, min - 1 + (k + 1) * d / n], while d = max - min + 1.
     * @param v
     * @return the number of buckets
     */
    private int valueToBucket(int v) {
        if (v < min || v > max) {
            throw new IllegalArgumentException();
        }
        int d = max - min + 1;
        int l = 0;
        int r = buckets - 1;
        while (l != r) {
            int mid = (l + r) / 2;
            if (min - 1 + (mid + 1) * d / buckets < v) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        return l;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        counts[valueToBucket(v)]++;
        total++;
    }

    /**
     * The probability that prob(x <= v).
     * @param v
     * @return the probability
     */
    public double probDist(int v) {
        if (v < min) {
            return 0;
        }
        if (v > max) {
            return 1;
        }
        int k = valueToBucket(v);
        double count = 0;
        for (int i = 0; i != k; i++) {
            count += counts[i];
        }
        int d = max - min + 1;
        int bucketLeft = min + k * d / buckets;
        int bucketRight = min - 1 + (k + 1) * d / buckets;
        count += (v - bucketLeft + 1) * counts[k] / (bucketRight - bucketLeft + 1);
        return count / (double)total;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS:
                return probDist(v) - probDist(v - 1);
            case NOT_EQUALS:
                return 1 - (probDist(v) - probDist(v - 1));
            case GREATER_THAN:
                return 1 - probDist(v);
            case GREATER_THAN_OR_EQ:
                return 1 - probDist(v - 1);
            case LESS_THAN:
                return probDist(v - 1);
            case LESS_THAN_OR_EQ:
                return probDist(v);
        }
        throw new IllegalArgumentException();
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("min = ");
        sb.append(min);
        sb.append(", max = ");
        sb.append(max);
        sb.append(", hist = [");
        for (int i = 0; i != counts.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(counts[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}
