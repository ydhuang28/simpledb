package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	/** Serialization. */
    private static final long serialVersionUID = 1L;
    
    /** Transaction id associated with this insert. */
    private TransactionId t;
    
    /** Child iterator. */
    private DbIterator child;
    
    /** Table id of table to insert the tuples into. */
    private int tableid;
    
    /** Flag for if the iterator is opened. */
    private boolean opened;
    
    /** Flag for if the tuples in the child iterator has been inserted. */
    private boolean inserted;
    
    /** Always {Type.INT_TYPE}. */
    private final TupleDesc td;
    

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        opened = false;
        inserted = false;
        Type[] tAr = {Type.INT_TYPE};
        String[] fNAr = {"no. of tuples inserted"};
    	td = new TupleDesc(tAr, fNAr);
    } // end Insert(TransactionId, DbIterator, int)
    
    
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
        inserted = false;
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
        inserted = false;
    } // end rewind()
    
    
    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (inserted) return null;
        if (!opened) throw new DbException("iterator not opened");
        
        int count = 0;
        try {
        	while (child.hasNext()) {
        		Database.getBufferPool().insertTuple(t, tableid, child.next());
        		count++;
        	}
        } catch (IOException ioe) {
        	ioe.printStackTrace();
        	throw new DbException("error when inserting child");
        }
        
        inserted = true;
        
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
} // end Insert