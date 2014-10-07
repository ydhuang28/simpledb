package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	/** Serialization. */
    private static final long serialVersionUID = 1L;
    
    /** Transaction id associated with this insert. */
    private TransactionId t;
    
    /** Child iterator. */
    private DbIterator child;
    
    /** Flag for if the iterator is opened. */
    private boolean opened;
    
    /** Flag for if the tuples in the child iterator has been inserted. */
    private boolean deleted;
    
    /** Always {Type.INT_TYPE}. */
    private final TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	this.t = t;
        this.child = child;
        opened = false;
        deleted = false;
        Type[] tAr = {Type.INT_TYPE};
        String[] fNAr = {"no. of tuples inserted"};
    	td = new TupleDesc(tAr, fNAr);
    } // end Delete(TransactionId, DbIterator)

    
    /**
     * @return TupleDesc associated with this insert.
     */
    public TupleDesc getTupleDesc() {
        return td;
    } // end getTupleDesc()

    
    /**
     * Opens the iterator. Call before rewind() and fetchNext().
     */
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        opened = true;
    } // end open()
    
    
    /**
     * Closes the iterator. After this, rewind() and fetchNext() will
     * throw exceptions.
     */
    public void close() {
    	super.close();
        child.close();
        opened = false;
        deleted = false;
    } // end close()

    
    /**
     * Rewinds the iterator.
     * 
     * @throws DbException if the iterator is not open
     * @throws TransactionAbortedException
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	if (!opened) {
        	throw new DbException("iterator not opened");
        }
        child.rewind();
        deleted = false;
    } // end rewind()

    
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
    	if (deleted) return null;
        if (!opened) throw new DbException("iterator not opened");
        
        int count = 0;
        try {
        	while (child.hasNext()) {
        		Database.getBufferPool().deleteTuple(t, child.next());
        		count++;
        	}
        } catch (IOException ioe) {
        	ioe.printStackTrace();
        	throw new DbException("error when deleting child");
        }
        
        deleted = true;
        
        Tuple rv = new Tuple(td);
        rv.setField(0, new IntField(count));
        return rv;
    } // end fetchNext()

    
    /**
     * @return array of children iterator(s)
     */
    @Override
    public DbIterator[] getChildren() {
        DbIterator[] rv = {child};
        return rv;
    } // end getChildren()

    
    /**
     * Sets the child to the child given in the array
     * 
     * @param children array of children iterators given
     */
    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    } // end setChildren(DbIterator[])

} // end Delete
