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
				entries.add(lastSharedGranted + 1,
						new LockTableEntry(isExclusive, false, tid));
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
	
	
	public synchronized void acquireLock(PageId pid, TransactionId tid, boolean excl) {
		if (!isLockFree(pid, tid, excl)) {
			lockTable.get(pid).add(new LockTableEntry(excl, false, tid));
		} else {
			LinkedList<LockTableEntry> entries = lockTable.get(pid);
			LockTableEntry newEntry = new LockTableEntry(excl, true, tid);
			if (!entries.contains(newEntry)) {	// for upgrade, already exists
				entries.add(new LockTableEntry(excl, true, tid));
			}
			return;
		}
		while (!isLockFree(pid, tid, excl)) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {}
		}
		// lock is free! grab it.
		lockTable.get(pid).add(new LockTableEntry(excl, true, tid));
	} // end acquireLock(PageId, TransactionId)
	
	
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
	
	
	public synchronized void releaseAllLocks(TransactionId tid) {
		for (PageId pid : lockTable.keySet()) {
			LinkedList<LockTableEntry> entries = lockTable.get(pid);
			
			for (int i = 0; i < entries.size(); i++) {
				LockTableEntry e = entries.get(i);
				if (e.tid.equals(tid)) {
					entries.remove(e);
				}
			}
		}
	} // end releaseAllLocks(TransactionId)
	
	
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		LinkedList<LockTableEntry> entries = lockTable.get(p);
		for (LockTableEntry e : entries) {
			if (e.tid.equals(tid) && e.isGranted) {
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

		LockTableEntry(boolean excl, boolean grnted, TransactionId tid) {
			isExclusive = excl;
			isGranted = grnted;
			this.tid = tid;
		} // end LockTableEntry(boolean, boolean)
		
	} // end LockTableEntry

} // end LockManager