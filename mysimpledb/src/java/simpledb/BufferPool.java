package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

	/** Private field for pagesize. */
    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. 90This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    
    /** Number of maximum pages in buffer. */
    public final int numPages;
    
    /** Buffer pool. */
    private final Page[] buffer;
    
    /** Array for time when each page entered pool. */
    private final long[] pageTime;
    
    /** Header to indicate which slots are used/not used. (bitmap) */
    private final byte[] bufHeader;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        if (numPages < 0) throw new RuntimeException("negative pages");
        
        buffer = new Page[numPages];
        pageTime = new long[numPages];
        bufHeader = new byte[(int) (Math.ceil(numPages / 8.0))];
        this.numPages = numPages;
    } // end BufferPool(int)

    
    /**
     * @return pagesize of each page in the buffer pool.
     */
    public static int getPageSize() {
        return pageSize;
    } // end getPageSize()

    
    /**
     * Sets the page size.
     * 
     * THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
     * 
     * @param pageSize size of page to set to
     */
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    } // end setPageSize(int)

    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        
    	for (int i = 0; i < buffer.length; i++) {
    		if (buffer[i] != null) {
    			if (pid.equals(buffer[i].getId())) {
    				return buffer[i];
    			}
    		}
    	}
    	
    	Catalog ctlg = Database.getCatalog();
    	DbFile dbfile = ctlg.getDatabaseFile(pid.getTableId());
    	
    	int emptyI = findEmptySlot();
    	while (emptyI < 0) {
    		evictPage();
    		emptyI = findEmptySlot();
    	}
    	
    	buffer[emptyI] = dbfile.readPage(pid);
    	pageTime[emptyI] = System.currentTimeMillis();
    	
    	return buffer[emptyI];
    } // end getPage(TransactionId, PageId, Permissions)

    
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    } // end releasePage(TransactionId, PageId)
    

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    } // end transactionComplete(TransactionId)

    
    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
        return false;
    } // end holdsLock(TransactionId, PageId)

    
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    } // end transactionComplete(TransactionId, boolean)

    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);

        // insert tuple
        ArrayList<Page> modified = f.insertTuple(tid, t);
        
        // mark modified page dirty
        modified.get(0).markDirty(true, tid);
        
        // update cached version(s)
        for (int i = 0; i < buffer.length; i++) {
        	if (buffer[i].getId().equals(modified.get(0).getId())) {
        		buffer[i] = modified.get(0);
        		return;
        	}
        }
    }

    
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	// iterate over all the tableids
    	Iterator<Integer> tidIter = Database.getCatalog().tableIdIterator();
    	
    	ArrayList<Page> plist = null;
    	while (tidIter.hasNext()) {
    		int tableId = tidIter.next();
    		DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    		
    		// try deleting the tuple from the current table
    		try {
    			plist = f.deleteTuple(tid, t);
    		} catch (DbException dbe) {
    			continue;
    		}
    		
    	}
    	
    	// mark dirty bit
    	if (plist == null) {
    		throw new DbException("could not delete tuple");
    	}
    	Page p = plist.get(0);
    	p.markDirty(true, tid);
    }

    
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (int i = 0; i < buffer.length; i++) {
        	if (!isSlotUsed(i)) {
        		flushPage(buffer[i].getId());
        	}
        }
    }

    
    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    
    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	// find page
    	Page pToFlush = null;
    	for (Page p : buffer) {
    		if (p.getId().equals(pid))
    			pToFlush = p;
    	}
    	
    	// write page to disk
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        f.writePage(pToFlush);
        pToFlush.markDirty(false, new TransactionId());
    } // end flushPage(PageId)

    
    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
    	// not necessary for lab1|lab2|lab3|lab4
    } // end flushPages(TransactionId)

    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        int earliestInd = 0;
    	for (int i = 0; i < pageTime.length; i++) {			// find index of earliest
        	if (pageTime[i] < pageTime[earliestInd])		// loaded page
        		earliestInd = i;
        }
    	
    	try {
    		flushPage(buffer[earliestInd].getId());			// try flushing
    	} catch (IOException ioe) {
    		throw new DbException("could not evict page");	// throw exception if fail
    	}
    	markSlot(earliestInd, false);						// mark page not used
    } // end evictPage()
    
    
    /**
     * Abstraction to find empty slot in BufferPool.
     * 
     * @return  the index of the empty slot (if any), or -1
     * 			if no empty slots
     */
    private int findEmptySlot() {
    	int emptyI = 0;
    	while (isSlotUsed(emptyI++));
    	if (emptyI >= numPages) {
    		return -1;
    	}
    	return emptyI;
    }
    
    /**
     * Abstraction for marking slot
     * 
     * @param slotI
     * @param isdirty
     */
    private void markSlot(int i, boolean isdirty) {
    	// calculate bit position and header byte
    	int bitPos = 1 << (i % Byte.SIZE);
    	int bytePos = i / Byte.SIZE;
    	
    	// create mask depending on value
    	byte mask = (byte) (isdirty ? bitPos : ~bitPos);
    	
    	// bitwise-OR or AND depending on value
    	if (isdirty) {
    		bufHeader[bytePos] = (byte) (bufHeader[bytePos] | mask);
    	} else {
    		bufHeader[bytePos] = (byte) (bufHeader[bytePos] & mask);
    	}
    }
    
    private boolean isSlotUsed(int i) {
    	if (i < 0 || i >= numPages) return false;
        
        int bitPos = i % Byte.SIZE;
    	byte mask = (byte) (1 << bitPos);
        
        return ((bufHeader[i / Byte.SIZE] & mask) >> bitPos) == 1;
    }

} // end BufferPool
