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
    }

    
    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return osFile;
    }

    
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
    }

    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    
   /**
    * See general contract in DbFile.java.
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
    }

    
    /**
     * See general contract in DbFile.java.
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    
    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) osFile.length() / BufferPool.getPageSize();
    }

    
    /**
     * See general contract in DbFile.java.
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    
    /**
     * See general contract in DbFile.java.
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    
    /**
     * See general contract in DbFile.java.
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this, td);
    }
    
    
    /**
     * Inner iterator class to iterate over all tuples in pages
     * in this HeapFile.
     * 
     * @author Yuxin David Huang
     */
    private class HeapFileIterator implements DbFileIterator {
    	
    	/** Current page number in the file. */
    	private int currPgNo;
    	
    	private Iterator<Tuple> currPgItr;
    	
    	/** Transaction id given by caller. */
    	private TransactionId tid;
    	
    	/**
    	 * Indicates whether the iterator has been opened.
    	 * hasNext(), next(), and rewind() will not work
    	 * if this is false.
    	 */
    	private boolean opened;
    	
    	/** The TupleDesc associated with tuples in this file. */
    	private TupleDesc td;
    	
    	/** This heap file. */
    	private HeapFile hf;
    	
    	/** Override default constructor. */
    	private HeapFileIterator() {}
    	
    	
    	/**
    	 * Constructs a new iterator for this HeapFile.
    	 * 
    	 * @param tid 	transaction id provided by caller
    	 * @param f		file to iterate over
    	 * @param td	TupleDesc for the file to iterate over
    	 */
    	private HeapFileIterator(TransactionId tid, HeapFile hf, TupleDesc td) {
    		currPgNo = 0;
    		opened = false;
    		this.td = td;
    		this.tid = tid;
    		this.hf = hf;
    	} // end HeapFileIterator(TransactionId, File, TupleDesc)
    	
    	
    	/**
    	 * Opens the iterator. Must be called before hasNext(), next(),
    	 * and rewind() can be called.
    	 */
    	public void open() throws DbException, TransactionAbortedException {
    		HeapPageId hpid = new HeapPageId(hf.getId(), currPgNo);
    		HeapPage hp = (HeapPage) Database.getBufferPool()
				  	.getPage(tid, hpid, Permissions.READ_ONLY);
    		
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
    		
    		if (currPgNo == hf.numPages() - 1 && !currPgItr.hasNext()) {
    			return false;
    		}
    		
    		return true;
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
    		
    		if (currPgItr.hasNext()) return currPgItr.next();
    		else {
    			currPgNo++;
    			HeapPageId hpid = new HeapPageId(hf.getId(), currPgNo);
        		currPgItr = ((HeapPage) Database.getBufferPool()
        				  	.getPage(tid, hpid, Permissions.READ_ONLY)).iterator();
        		return currPgItr.next();
    		}
    	} // end next()
    	
    	
    	/**
    	 * Resets the iterator to start.
    	 * See general contract in DbIterator.java.
    	 */
    	public void rewind() throws DbException, TransactionAbortedException {
    		if (!opened) throw new IllegalStateException("iterator not opened");
    		
    		currPgNo = 0;
    		HeapPageId hpid = new HeapPageId(hf.getId(), currPgNo);
    		currPgItr = ((HeapPage) Database.getBufferPool()
    				  	.getPage(tid, hpid, Permissions.READ_ONLY)).iterator();
    		
    	} // end rewind()
    	
    	/**
    	 * Returns the TupleDesc associated with the file being iterated over.
    	 * See general contract in DbIterator.java.
    	 * 
    	 * @return the TupleDesc associated with the file
    	 */
    	public TupleDesc getTupleDesc() {
    		return td;
    	} // end getTupleDesc()
    	
    	/**
    	 * Closes the iterator. After this, hasNext(), next(), and rewind()
    	 * will fail.
    	 * 
    	 * See general contract in DbIterator.java.
    	 */
    	public void close() {
    		opened = false;
    	} // end close()
    } // end HeapFileIterator
} // end HeapFile

