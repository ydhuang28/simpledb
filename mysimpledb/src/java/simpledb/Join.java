package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	/** Serialization. */
    private static final long serialVersionUID = 1L;

    /** Predicate on which the 2 child is joined. */
    private JoinPredicate p;
    
    /** Children iterator of this join. */
    private DbIterator[] children;
    
    /** Flag for whether the iterator is open. */
    private boolean opened;
    
    private Tuple next1;
    private Tuple next2;
    
    private String fieldName1;
    private String fieldName2;
    
    
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
    	this.p = p;
    	children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        opened = false;
        next1 = null;
        next2 = null;
        
        // initialize field names
        String[] fNames = getFieldNames(child1, child2);
        fieldName1 = fNames[0];
        fieldName2 = fNames[1];
    } // end Join(JoinPredicate, DbIterator, DbIterator)

    
    /**
     * @return the predicate associated with this join
     */
    public JoinPredicate getJoinPredicate() {
        return p;
    } // end getJoinPredicate()

    
    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
    	return fieldName1;
    } // end getJoinField1Name

    
    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
    	return fieldName2;
    } // end getJoinField2Name()
    

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(children[0].getTupleDesc(),
        					   children[1].getTupleDesc());
    } // end getTupleDesc()

    
    /**
     * Opens the iterator. Call this before calling rewind()
     * and fetchNext().
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        children[0].open();
        children[1].open();
        opened = true;
    } // end open()

    
    /**
     * Closes the iterator. After this, rewind() and fetchNext()
     * no longer works.
     */
    public void close() {
        super.close();
        children[0].close();
        children[1].close();
        opened = false;
    } // end close()

    
    /**
     * Rewinds the iterator.
     * 
     * @throws DbException if iterator not opened.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        if (!opened) {
        	throw new DbException("iterator not opened");
        }
        children[0].rewind();
        children[1].rewind();
        next1 = null;
        next2 = null;
    } // end rewind()

    
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p/>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p/>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!opened) throw new DbException("iterator not opened");
        
        // check for empty table
        if (!children[0].hasNext() || !children[1].hasNext()) {
        	if (next1 == null || next2 == null) {
        		return null;
        	}
        }
    	
        // look for tuple
    	while (true) {
    		if (next1 == null && children[0].hasNext()) {
    			next1 = children[0].next();
    		}
    		while (children[1].hasNext()) {
    			next2 = children[1].next();
    			if (p.filter(next1, next2)) {
    				return merge(next1, next2);
    			}
    		}
    		children[1].rewind();
    		if (children[0].hasNext()) {
    			next1 = children[0].next();
    		} else break;
    	}
        return null;
    } // end fetchNext()

    
    /**
     * Returns the children iterator of this join.
     */
    @Override
    public DbIterator[] getChildren() {
        return children;
    } // end getChildren()

    
    @Override
    public void setChildren(DbIterator[] children) {
        this.children = children;
        
        String[] fNames = getFieldNames(children[0], children[1]);
        fieldName1 = fNames[0];
        fieldName2 = fNames[1];
    } // end setChildren(DbIterator[])
    
    
    private String[] getFieldNames(DbIterator i1, DbIterator i2) {
    	String fN1 = i1.getTupleDesc().getFieldName(p.getField1());
    	String fN2 = i2.getTupleDesc().getFieldName(p.getField2());
    	String[] rv = {fN1, fN2};
    	return rv;
    }
    
    
    private Tuple merge(Tuple t1, Tuple t2) {
    	TupleDesc td = TupleDesc.merge(next1.getTupleDesc(),
    			next2.getTupleDesc());
    	Tuple rv = new Tuple(td);
    	for (int i = 0; i < next1.getTupleDesc().numFields(); i++) {
    		rv.setField(i, next1.getField(i));
    	}
    	for (int j = next1.getTupleDesc().numFields();
    			j < td.numFields(); j++) {
    		rv.setField(j,
    				next2.getField(j - next1.getTupleDesc().numFields()));
    	}
    	return rv;
    }
} // end Join
