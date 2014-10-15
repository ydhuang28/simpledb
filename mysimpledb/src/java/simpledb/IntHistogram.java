package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	/** max value in the histogram. */
	private final int max;
	
	/** min value in the histogram. */
	private final int min;
	
	/** width of each bucket, except for the last. */
	private final int width;
	
	/** width of the last bucket. */
	private final int lstWidth;
	
	/** count of each bucket. */
	private final int[] buckets;
	
	/** sum of counts of all baskets. */
	private int numTuples;

    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // set min and max
        this.min = min;
        this.max = max;
        
        // calculate # of buckets
        if (max-min+1 < buckets) {
        	this.buckets = new int[max-min+1];
        	width = 1;
        	lstWidth = 1;
        } else {
        	this.buckets = new int[buckets];
        	width = (int) Math.floor((max-min+1)/(double) buckets);
        	lstWidth = (max-(min+(buckets-1)*width)) + 1;
        }
        
        // set everything to 0, for safety
        for (int i = 0; i < this.buckets.length; i++) {
        	this.buckets[i] = 0;
        }
        
        // set numTuples to 0
        numTuples = 0;
    } // end IntHistogram(int, int, int)
    

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// argument checking
        if (v < min || v > max) {
        	throw new IllegalArgumentException("value to add out of range");
        }
        
        if ((v-min)/width < buckets.length-1) {
        	buckets[(v-min)/width]++;		// not last bucket
        } else {
        	buckets[buckets.length-1]++;	// last bucket gets all remainder
        }
        
        numTuples++;
    } // end addValue(int)
    

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
        	case EQUALS:
        		if (v < min || v > max) return 0;
        		
        		int b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		if (b < buckets.length-1) {
        			return (buckets[b]/(double) width)/numTuples;
        		} else {
        			return (buckets[b]/(double) lstWidth)/numTuples;
        		}
        	case LIKE:
        		if (v < min || v > max) return 0;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		if (b < buckets.length-1) {
        			return (buckets[b]/(double) width)/numTuples;
        		} else {
        			return (buckets[b]/(double) lstWidth)/numTuples;
        		}
        	case NOT_EQUALS:
        		if (v < min || v > max) return 1;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		if (b < buckets.length-1) {
        			return 1 - (buckets[b]/(double) width)/numTuples;
        		} else {
        			return 1 - (buckets[b]/(double) lstWidth)/numTuples;
        		}
        	case GREATER_THAN:
        		if (v < min) return 1;
        		else if (v >= max) return 0;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        		
        		double b_f = buckets[b]/(double) numTuples;
        		if (b < buckets.length-1) {
        			double rv = ((min+(b+1)*width-1-v)/(double) width) * b_f;
        			for (int i = b+1; i < buckets.length; i++) {
        				rv += buckets[i]/(double) numTuples;
        			}
        			return rv;
        		} else {
        			return ((max-v)/(double) lstWidth) * b_f;
        		}
        	case LESS_THAN_OR_EQ:
        		if (v < min) return 0;
        		else if (v >= max) return 1;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		b_f = buckets[b]/(double) numTuples;
        		if (b < buckets.length-1) {
        			b_f = buckets[b]/(double) numTuples;
        			double rv = ((min+(b+1)*width-1-v)/(double) width) * b_f;
        			for (int i = b+1; i < buckets.length; i++) {
        				rv += buckets[i]/(double) numTuples;
        			}
        			return 1 - rv;
        		} else {
        			return 1 - ((max-v)/(double) lstWidth) * b_f;
        		}
        	case LESS_THAN:
        		if (v <= min) return 0;
        		else if (v > max) return 1;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		b_f = buckets[b]/(double) numTuples;
        		if (b < buckets.length-1) {
        			double rv = (((min+(b+1)*width)-v-1)/(double) width) * b_f;
        			for (int i = b-1; i >= 0; i--) {
        				rv += buckets[i]/(double) numTuples;
        			}
        			return rv;
        		} else {
        			double rv = ((max-v)/(double) lstWidth) * b_f;
        			for (int i = b-1; i >= 0; i--) {
        				rv += buckets[i]/(double) numTuples;
        			}
        			return rv;
        		}
        	case GREATER_THAN_OR_EQ:
        		if (v <= min) return 1;
        		else if (v > max) return 0;
        		
        		b = (v-min)/width < buckets.length-1 ?
        				(v-min)/width : buckets.length-1;
        				
        		b_f = buckets[b]/(double) numTuples;
        		if (b < buckets.length-1) {
        			double rv = (((min+(b+1)*width)-v)/(double) width) * b_f;
        			for (int i = b+1; i < buckets.length; i++) {
        				rv += buckets[i]/(double) numTuples;
        			}
        			return rv;
        		} else {
        			return ((max-v+1)/(double) lstWidth) * b_f;
        		}
        	default:
        		throw new IllegalArgumentException("op not supported");
        }
    } // end estimateSelectivity(Predicate.Op, int)

    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String rv = "All buckets are right exclusive except for the last one:\r\n";
        for (int i = 0; i < buckets.length; i++) {
        	if (i < buckets.length-1) {
        		rv += "  " + (min+i*(max-min)) + "-" + (min+(i+1)*(max-min))
        				+ ": " + buckets[i] + "\r\n";
        	} else {
        		rv += "  " + (min+i*(max-min)) + "-" + max
        				+ ": " + buckets[i];
        	}
        }
        
        return rv;
    } // end toString()
} // end IntHistogram
