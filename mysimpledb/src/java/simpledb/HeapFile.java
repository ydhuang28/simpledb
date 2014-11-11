package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	/** Underlying OS file. */
	private File osFile;
	
	/** Description of tuples stored in this table/heapfile. */
	private TupleDesc td;
	
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
    	osFile = f;
        this.td = td;
    } // end HeapFile(File, TupleDesc)

    
    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return osFile;
    } // end getFile()

    
    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return osFile.getAbsoluteFile().hashCode();
    } // end getId()

    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    } // end getTupleDesc()

    
   /**
    * @see DbFile#readPage(PageId)
    */
    public Page readPage(PageId pid) {
    	int ps = BufferPool.getPageSize();
    	int offset = pid.pageNumber() * ps;
    	
    	// start file handling
    	try {
    		// create stream
    		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(osFile));
    		
    		// create byte array, read data
    		byte[] data = new byte[ps];
    		bis.skip(offset);
    		bis.read(data, 0, ps);
    		
    		HeapPageId hpid = new HeapPageId(getId(), pid.pageNumber());
    		Page page = new HeapPage(hpid, data);
    		
    		bis.close();
    		return page;
    	} catch (FileNotFoundException fnfe) {
    		throw new IllegalArgumentException("file not found");
    	} catch (IOException ioe) {
    		throw new IllegalArgumentException("error when reading file");
    	}
    } // end readPage(PageId)

    
    /**
     * @see DbFile#writePage(Page)
     */
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(osFile, "rw");
        
        int offset = page.getId().pageNumber() * BufferPool.getPageSize();
        raf.skipBytes(offset);
        raf.write(page.getPageData());
        raf.close();
    } // end writePage(Page)

    
    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(osFile.length() / BufferPool.getPageSize());
    } // end numPages()
    
    int pagesCreated = 0;
    
    /**
     * @see DbFile#insertTuple(TransactionId, Tuple)
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	// argument checking
    	if (!td.equals(t.getTupleDesc())) {
    		throw new DbException("TupleDesc mismatch");
    	}
    	
    	// look for page with empty slot(s)
    	int pgNo = 0;
    	ArrayList<Page> rv = new ArrayList<Page>();
    	for (; pgNo < this.numPages(); pgNo++) {
    		HeapPageId pid = new HeapPageId(getId(), pgNo);
    		HeapPage page = (HeapPage) Database.getBufferPool()
    				.getPage(tid, pid, Permissions.READ_WRITE);
    		
    		// find page with empty slot
    		if (page.getNumEmptySlots() != 0) {
    			assert page.getNumEmptySlots() != 0;
    			page.insertTuple(t);
    			rv.add(page);
    			return rv;
    		}
    	}
    	
    	// no page with empty slot! create new page
    	HeapPage newpage = new HeapPage(new HeapPageId(getId(), pgNo),
    			HeapPage.createEmptyPageData());
    	newpage.insertTuple(t);
    	
    	// begin write out
    	BufferedOutputStream bos = new BufferedOutputStream(
    			new FileOutputStream(osFile, true));
    	bos.write(newpage.getPageData());
    	bos.flush();
    	bos.close();
    	
    	// return modified page
    	rv.add(newpage);
        return rv;
    } // end insertTuple(TransactionId, Tuple)

    
    /**
     * @see DbFile#deleteTuple(TransactionId, Tuple)
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	// argument checking
    	if (getId() != t.getRecordId().getPageId().getTableId()) {
    		throw new DbException("tuple not member of file");
    	}
        
    	HeapPage page = (HeapPage) Database.getBufferPool()
    			.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	
    	ArrayList<Page> returned = new ArrayList<Page>();
    	returned.add(page);
        return returned;
        
    } // end deleteTuple(TransactionId, Tuple)

    
    /**
     * @see DbFile#iterator(TransactionId)
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    } // end iterator(TransactionId)
    
    
    /**
     * Inner iterator class to iterate over all tuples in pages
     * in this HeapFile.
     * 
     * @author Yuxin David Huang
     */
    private class HeapFileIterator implements DbFileIterator {
    	
    	/** Current page number in the file. */
    	private int currPgNo;
    	
    	/** Iterator of current page. */
    	private Iterator<Tuple> currPgItr;
    	
    	/** Transaction id given by caller. */
    	private TransactionId tid;
    	
    	/**
    	 * Indicates whether the iterator has been opened.
    	 * hasNext(), next(), and rewind() will not work
    	 * if this is false.
    	 */
    	private boolean opened;
    	
    	/** Read only or read/write for some given instance */
    	private Permissions permission;
    	
    	/** Override default constructor. */
    	private HeapFileIterator() {}
    	
    	
    	/**
    	 * Constructs a new iterator for this HeapFile.
    	 * 
    	 * @param tid	transaction id provided by caller
    	 * @param p		permission of the iterator
    	 */
    	private HeapFileIterator(TransactionId tid, Permissions p) {
    		currPgNo = 0;
    		opened = false;
    		this.tid = tid;
    		permission = p;
    	}
    	
    	
    	/**
    	 * Constructs a new iterator for this HeapFile.
    	 * Use default permission (read only).
    	 * 
    	 * @param tid 	transaction id provided by caller
    	 * @param f		file to iterate over
    	 * @param td	TupleDesc for the file to iterate over
    	 */
    	private HeapFileIterator(TransactionId tid) {
    		this(tid, Permissions.READ_ONLY);
    	} // end HeapFileIterator(TransactionId, File, TupleDesc)
    	
    	
    	/**
    	 * Opens the iterator. Must be called before hasNext(), next(),
    	 * and rewind() can be called.
    	 */
    	public void open() throws DbException, TransactionAbortedException {
    		HeapPageId hpid = new HeapPageId(getId(), currPgNo);
    		HeapPage hp = (HeapPage) Database.getBufferPool()
				  	.getPage(tid, hpid, permission);
    		
    		currPgItr = hp.iterator();
    		opened = true;
    	} // end open()
    	
    	
    	/**
    	 * Checks whether there are more tuples in the file that can be read.
    	 * See general contract in DbIterator.java.
    	 * 
    	 * @return true if there are more tuples in this file
    	 */
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (!opened) return false;
    		if (currPgItr.hasNext()) return true;
    		
    		currPgNo++;
    		
    		while (currPgNo < numPages()) {
    			HeapPage p = (HeapPage) Database.getBufferPool().getPage(
    					tid, new HeapPageId(getId(), currPgNo), permission);
    			
    			currPgItr = p.iterator();
    			if (currPgItr.hasNext()) return true;
    			else currPgNo++;
    		}
    		
    		return false;
    	} // end hasNext()
    	
    	
    	/**
    	 * Returns the next tuple in this file.
    	 * See general contract in DbIterator.java.
    	 * 
    	 * @return the next tuple in this file
    	 */
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if (!opened) throw new NoSuchElementException("iterator not opened");
    		
    		if (!hasNext()) throw new NoSuchElementException("no more tuples");
    		
    		return currPgItr.next();
    	} // end next()
    	
    	
    	/**
    	 * Resets the iterator to start.
    	 * See general contract in DbIterator.java.
    	 */
    	public void rewind() throws DbException, TransactionAbortedException {
    		close();
    		open();    		
    	} // end rewind()
    	
    	
    	/**
    	 * Closes the iterator. After this, hasNext(), next(), and rewind()
    	 * will fail.
    	 * 
    	 * See general contract in DbIterator.java.
    	 */
    	public void close() {
    		currPgNo = 0;
    		currPgItr = null;
    		opened = false;
    	} // end close()
    	
    	
    	/**
    	 * This iterator does not support remove operation.
    	 * 
    	 * @throws UnsupportedOperationException
    	 */
    	@SuppressWarnings("unused")
		public void remove() throws UnsupportedOperationException {
    		throw new UnsupportedOperationException("removed not supported");
    	} // end remove()
    } // end HeapFileIterator
} // end HeapFile

