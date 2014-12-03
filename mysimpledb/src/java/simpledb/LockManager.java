package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

/**
 * A class that acts as a lock manager for every transaction in SimpleDB.
 * 
 * @author Yuxin David Huang '16, Colgate University
 */
public class LockManager {
	
	/** The number of milliseconds that a transaction has before timing out. */
	public static final long TIMEOUT = 600;	// 0.6 seconds
	
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
	 * Abstraction to check whether a lock of a page is free.
	 * 
	 * ONLY CALL THIS METHOD IN A SYNCHRONIZED METHOD!!!!!
	 * 
	 * @param lock pageid for the page to which the lock belongs
	 * @param isExclusive whether the request is exclusive
	 * @return whether the lock is free given input
	 */
	private boolean isLockFree(PageId lock, TransactionId tid, boolean isExclusive) {
		LinkedList<LockTableEntry> entries = lockTable.get(lock);
		
		// if no entries then yes
		if (entries == null) {
			lockTable.put(lock, new LinkedList<LockTableEntry>());
			return true;
		}
		
		// if any lock holder is exclusive, lock is not free
		// if lock holder is same as the one requesting, lock is free
		boolean upgrade = false;
		LockTableEntry upgradeToRemove = null;
		int lastSharedGranted = -1;
		for (int i = 0; i < entries.size(); i++) {
			LockTableEntry e = entries.get(i);
			if (e.tid.equals(tid)) {
				if (e.isGranted) {
					// upgrade!
					if (!e.isExclusive && isExclusive) {
						upgrade = true;
						upgradeToRemove = e;
					} else {	// if duplicate request or write -> read, free
						return true;
					}
				}
			} else {
				// shared lock! record lastSharedGranted
				if (!e.isExclusive && e.isGranted) {
					lastSharedGranted = i;
				}
				
				// exclusive lock! not free
				if (e.isExclusive && e.isGranted) {
					return false;
				}
			}
		}
		
		if (upgrade) {
			if (lastSharedGranted != -1) {
				entries.remove(upgradeToRemove);
				if (lastSharedGranted == entries.size()) {
					entries.add(new LockTableEntry(isExclusive, false, tid));
				} else { 
					entries.add(lastSharedGranted + 1,
							new LockTableEntry(isExclusive, false, tid));
				}
				return false;
			}
			else return true;
		}
		
		// if any lock holder exists and the request is
		// for an exclusive lock, lock is not free
		if (!entries.isEmpty() && isExclusive) return false;
		
		// otherwise lock is free
		return true;
	} // end isLockFree(PageId, boolean)
	
	
	/**
	 * Tries to acquire a lock (type given by <code>excl</code>)
	 * on a given page 
	 * 
	 * @param pid the page that the transaction tries to obtain a lock on
	 * @param tid the given transaction
	 * @param excl whether the lock to acquire is exclusive
	 */
	public synchronized void acquireLock(PageId pid,
										 TransactionId tid,
										 boolean excl)
			throws TransactionAbortedException, DbException {
		
		if (!isLockFree(pid, tid, excl)) {
			LinkedList<LockTableEntry> entries = lockTable.get(pid);
			LockTableEntry newEntry = new LockTableEntry(excl, false, tid);
			if (!entries.contains(newEntry)) {
				entries.add(newEntry);
			}
		} else {
			LinkedList<LockTableEntry> entries = lockTable.get(pid);
			LockTableEntry newEntry = new LockTableEntry(excl, true, tid);
			if (!entries.contains(newEntry)) {	// for upgrade, already exists
				entries.add(newEntry);
			}
			return;
		}
		
		// timeouts; deadlock resolution
		long timeWaited = 0;
		long lastChecked = System.currentTimeMillis();
		while (!isLockFree(pid, tid, excl)) {
			try {
				Thread.sleep(1);
				long now = System.currentTimeMillis();
				long elapsed = now - lastChecked;
				timeWaited += elapsed;
				if (timeWaited >= TIMEOUT) {
					throw new TransactionAbortedException();
				}
				lastChecked = now;
			} catch (InterruptedException e) {}
		}
		
		// lock is free! grab it.
		lockTable.get(pid).add(new LockTableEntry(excl, true, tid));
	} // end acquireLock(PageId, TransactionId)
	
	
	/**
	 * Releases a certain lock on a page that a transaction holds.
	 * 
	 * @param pid the page that the given transaction holds lock on
	 * @param tid TransactionId of the given transaction
	 */
	public synchronized void releaseLock(PageId pid, TransactionId tid) {
		LinkedList<LockTableEntry> entries = lockTable.get(pid);
		
		LockTableEntry target = null;
		for (LockTableEntry e : entries) {
			if (e.tid.equals(tid)) {
				target = e;
			}
		}
		
		if (target != null) {
			entries.remove(target);
		}
	} // end releaseLock(PageId, TransactionId)
	
	
	/**
	 * Releases all locks, granted or not, held by a transaction.
	 * 
	 * This is only called when transaction commits/aborts.
	 * 
	 * @param tid TransactionId of transaction of which to release all locks
	 */
	public synchronized void releaseAllLocks(TransactionId tid) {
		for (PageId pid : lockTable.keySet()) {
			LinkedList<LockTableEntry> entries = lockTable.get(pid);
			
			HashSet<LockTableEntry> toRemove = new HashSet<LockTableEntry>();
			for (LockTableEntry e : entries) {
				if (e.tid.equals(tid)) {
					toRemove.add(e);
				}
			}
			
			for (LockTableEntry e : toRemove) {
				entries.remove(e);
			}
		}
	} // end releaseAllLocks(TransactionId)
	
	
	/**
	 * Checks whether a given transaction holds a lock on a
	 * given page or not.
	 * 
	 * @param tid TransactionId of the given transaction
	 * @param p PageId of the page to check
	 * @return true if tid holds lock on p, false otherwise
	 */
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		LinkedList<LockTableEntry> entries = lockTable.get(p);
		for (LockTableEntry e : entries) {
			if (e.tid.equals(tid) && e.isGranted) {
				return true;
			}
		}
		return false;
	} // end holdsLock(TransactionId, PageId)
	
	
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

		/**
		 * Constructor. 
		 */
		LockTableEntry(boolean excl, boolean grnted, TransactionId tid) {
			isExclusive = excl;
			isGranted = grnted;
			this.tid = tid;
		} // end LockTableEntry(boolean, boolean)
		
	} // end LockTableEntry

} // end LockManager