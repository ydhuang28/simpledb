package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

	/** Serialization. */
    private static final long serialVersionUID = 1L;
    
    /** Predicate associated with this select. */
    private Predicate p;
    
    /** Child iterator/previous operator in pipeline. */
    private DbIterator child;
    
    /** Flag indicating whether the iterator is open. */
    private boolean opened;
    
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
        opened = false;
    } // end Filter(Predicate, DbIterator)

    
    /**
     * @return the predicate associated with this select.
     */
    public Predicate getPredicate() {
        return p;
    } // end getPredicate()

    
    /**
     * @return TupleDesc associated with the tuples returned by this select.
     */
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    } // end getTupleDesc()

    
    /**
     * Opens the iterator.
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	child.open();
    	super.open();
        opened = true;
    } // end open()

    
    /**
     * Closes the iterator.
     */
    public void close() {
        opened = false;
        super.close();
        child.close();
    } // end close()

    
    /**
     * Rewinds the iterator; resets pointer to the first tuple.
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	if (!opened) {
    		throw new DbException("iterator not opened");
    	}
        child.rewind();
    } // end rewind()

    
    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!opened) {
        	throw new DbException("iterator not opened");
        }
        
        Tuple next = null;
        
        // get next tuple
        while (child.hasNext()) {
        	next = child.next();
        	if (!p.filter(next)) next = null;	// filter tuple
        	if (next != null) break;			// break if found
        }
        
        return next;
    } // end fetchNext()

    
    /**
     * @see Operator#getChildren()
     */
    @Override
    public DbIterator[] getChildren() {
    	DbIterator[] rv = {child};
        return rv;
    } // end getChildren()

    
    /**
     * @see Operator#setChildren(DbIterator[])
     */
    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    } // end setChildren(DbIterator[])
} // end Filter