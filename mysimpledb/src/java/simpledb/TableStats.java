package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap
    	= new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    } // end getTableStats(String)

    
    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    } // end setTableStats(String, TableStats)

    
    /**
     * Sets the stats map.
     * 
     * @param s the stats map to set to
     */
    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF
            	= TableStats.class.getDeclaredField("statsMap");
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

    } // end setStatsMap(HashMap<String, TableStats>)

    
    /**
     * @return the stats map associated
     */
    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    } // end getStatsMap()

    
    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    } // end computeStatistics()

    
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private int numTuples;
    private int scanIOCost;
    private int[] distinctValues;
    private Object[] histograms;

    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	DbFile f = Database.getCatalog().getDatabaseFile(tableid);
    	DbFileIterator iter = f.iterator(new TransactionId());
    	TupleDesc td = f.getTupleDesc();
    	int numFields = td.numFields();
    	
    	// set up instance variables
    	scanIOCost = ((HeapFile) f).numPages() * ioCostPerPage;
    	numTuples = 0;
    	distinctValues = new int[numFields];
    	histograms = createHistograms(td, iter);
    	
    	try {
    		iter.open();
    		iter.rewind();
    		
    		// store distinct values for each field
    		ArrayList<HashSet<Field>> distinctFields
    			= new ArrayList<HashSet<Field>>();
    		for (int i = 0; i < numFields; i++) {
    			distinctFields.add(new HashSet<Field>());
    		}
			while (iter.hasNext()) {
				// count number of tuples (cardinality)
				Tuple curr = iter.next();
				numTuples++;
				
				// count distinct values for each field;
				// add tuples into histogram
				for (int i = 0; i < numFields; i++) {
					HashSet<Field> dfs = distinctFields.get(i);
					Field currF = curr.getField(i);
					
					// count distinct values
					if (!dfs.contains(currF)) {
						dfs.add(currF);
						distinctValues[i]++;
					}
					
					// check int or string field, and
					// add value accordingly
					if (td.getFieldType(i).equals(Type.INT_TYPE)) {
						IntHistogram hist = (IntHistogram) histograms[i];
						hist.addValue(((IntField) curr.getField(i)).getValue());
					} else {
						StringHistogram hist = (StringHistogram) histograms[i];
						hist.addValue(((StringField) curr.getField(i)).getValue());
					}
				}
			}
			iter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    } // end TableStats(int, int)
    
    
    /**
     * Helper method to create histograms.
     * 
     * @param histgrms array of histograms to store the
     * 				   created ones in
     * @param numFields number of fields in the tuple
     * @param iter iterator over all tuples
     */
    private Object[] createHistograms(TupleDesc td,
    							  	  DbFileIterator iter) {
    	
    	int numFields = td.numFields();
    	
    	Object[] rv = new Object[numFields];
    	int[] maxs = new int[numFields];
    	int[] mins = new int[numFields];
    	for (int i = 0; i < numFields; i++) {
    		maxs[i] = Integer.MIN_VALUE;
    		mins[i] = Integer.MAX_VALUE;
    	}
    	
    	// get min and max if int fields
    	// if string fields, nothing is done,
    	// since min and max is preset
    	try {
    		iter.open();
			while (iter.hasNext()) {
				Tuple t = iter.next();
				
				for (int i = 0; i < numFields; i++) {
					Field f = t.getField(i);
					Type ft = td.getFieldType(i);
		    		if (ft.equals(Type.INT_TYPE)) {
		    			if (f.compare(Predicate.Op.GREATER_THAN_OR_EQ, new IntField(maxs[i]))) {
		    				maxs[i] = ((IntField) f).getValue();
		    			} else if (f.compare(Predicate.Op.LESS_THAN_OR_EQ, new IntField(mins[i]))) {
		    				mins[i] = ((IntField) f).getValue();
		    			}
		    		}
		    	}
				
			}
			iter.close();
		} catch (Exception e) {}
    	
    	// create histograms
    	for (int i = 0; i < numFields; i++) {
    		if (td.getFieldType(i).equals(Type.INT_TYPE)) {
    			rv[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
    		} else {
    			rv[i] = new StringHistogram(NUM_HIST_BINS);
    		}
    	}
    	
    	return rv;
    	
    } // end createHistograms(Object[], int, DbIterator)

    
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return scanIOCost;
    } // end estimateScanCost()

    
    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.ceil(numTuples * selectivityFactor);
    } // end estimateTableCardinality(double)

    
    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
        return distinctValues[field];
    } // end numDistinctValues(int)

    
    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Object hist = histograms[field];
        if (hist.getClass().equals(IntHistogram.class)) {
        	return ((IntHistogram) hist).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
        	return ((StringHistogram) hist).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    } // end estimateSelectivity(int, Predicate.Op, Field)

} // end TableStats