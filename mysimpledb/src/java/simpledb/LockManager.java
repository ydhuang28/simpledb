package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * A class that acts as a lock manager for every transaction in SimpleDB.
 * 
 * @author Yuxin David Huang '16, Colgate University
 */
public class LockManager {
	
	/** The number of milliseconds that a transaction has before timing out. */
	public static final long TIMEOUT = 2000;	// 2 seconds
	
	/**
	 * Lock table. Each page has a lock and a queue of transactions
	 * that are waiting on the lock. The boolean value is to distinguish
	 * whether a lock is exclusive or not exclusive.
	 */
	private Map<PageId, LinkedList<LockTableEntry>> lockTable;
	
	
	/**
	 * Constructor.
	 */
	public LockManager() {
		lockTable = new HashMap<PageId, LinkedList<LockTableEntry>>();
	} // end LockManager()
	
	
	/**
	 * Abstraction to check whether a lock of a page is free
	 * 
	 * @param lock pageid for the page to which the lock belongs
	 * @param isExclusive whether the request is exclusive
	 * @return
	 */
	private boolean isLockFree(PageId lock, boolean isExclusive) {
		LinkedList<LockTableEntry> entries = lockTable.get(lock);
		
		// if no entries then yes
		if (entries == null) {
			System.out.println("no entries!");
			lockTable.put(lock, new LinkedList<LockTableEntry>());
			return true;
		}
		
		// if any lock holder exists and the request is
		// for an exclusive lock, lock is not free
		if (!entries.isEmpty() && isExclusive) return false;
		
		// if any lock holder is exclusive, lock is not free
		for (LockTableEntry e : entries) {
			if (e.isExclusive()) {
				return false;
			}
		}
		
		// otherwise lock is free
		return true;
	} // end isLockFree(PageId, boolean)
	
	private void addToQueue(PageId lock, TransactionId tid, boolean excl) {
		
	} // end addToQueue(PageId, TransactionId, boolean)
	
	
	public void acquireLock(PageId pid, TransactionId tid, boolean excl) {
		synchronized (this) {
			System.out.println("checking");
			if (!isLockFree(pid, excl)) {
				lockTable.get(pid).add(new LockTableEntry(excl, false, tid));
			} else {
				lockTable.get(pid).add(new LockTableEntry(excl, true, tid));
				return;
			}
			while (!isLockFree(pid, excl)) {
				System.out.println("sleeping");
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {}
			}
			// lock is free! grab it.
			lockTable.get(pid).add(new LockTableEntry(excl, true, tid));
		}
	} // end acquireLock(PageId, TransactionId)
	
	
	public synchronized void releaseLock(PageId pid, TransactionId tid) {
		LinkedList<LockTableEntry> entries = lockTable.get(pid);
		
		LockTableEntry target = null;
		for (LockTableEntry e : entries) {
			if (e.getTransactionId().equals(tid)) {
				target = e;
			}
		}
		
		if (target != null) {
			entries.remove(target);
		}
	} // end releaseLock(PageId, TransactionId)
	
	
	public synchronized void releaseAllLocks(TransactionId tid) {
		throw new UnsupportedOperationException();
	} // end releaseAllLocks(TransactionId)
	
	
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		LinkedList<LockTableEntry> entries = lockTable.get(p);
		for (LockTableEntry e : entries) {
			if (e.getTransactionId().equals(tid) && e.isGranted()) {
				return true;
			}
		}
		return false;
	}
	
	
	/** 
	 * Abstraction to provide better access to an entry in the
	 * lock table.
	 * 
	 * @author Yuxin David Huang '16, Colgate University
	 */
	private class LockTableEntry {
		
		boolean isExclusive;
		boolean isGranted;
		final TransactionId tid;
		
		LockTableEntry() {
			isExclusive = false;
			isGranted = false;
			tid = null;
		}
		
		LockTableEntry(boolean excl, boolean grnted, TransactionId tid) {
			isExclusive = excl;
			isGranted = grnted;
			this.tid = tid;
		} // end LockTableEntry(boolean, boolean)
		
		boolean isExclusive() {
			return isExclusive;
		}
		
		void setExclusive(boolean v) {
			isExclusive = v;
		}
		
		boolean isGranted() {
			return isGranted;
		}
		
		void setGranted(boolean v) {
			isGranted = v;
		}
		
		TransactionId getTransactionId() {
			return tid;
		}
		
	} // end LockTableEntry

} // end LockManager