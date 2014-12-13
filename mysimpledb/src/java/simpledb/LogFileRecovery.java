package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
    	readOnlyLog.seek(readOnlyLog.length());
    	System.out.println("rollback? = " + tidToRollback);
        
        synchronized (Database.getBufferPool()) {
        	synchronized (this) {
        		long currentOffset = readOnlyLog.getFilePointer();
        		
        		// file starts with a long indicating last checkpoint;
        		// only need to check until there
        		while (currentOffset > LogFile.LONG_SIZE) {
        			
        			// read last long, find head of record, go there
        			readOnlyLog.seek(currentOffset - LogFile.LONG_SIZE);
        			long recordOffset = readOnlyLog.readLong();
        			readOnlyLog.seek(recordOffset);
        			
        			int type = readOnlyLog.readInt();
        			long tid = readOnlyLog.readLong();
        			
        			// only switch if the transaction is the one we want
        			if (tid == tidToRollback.getId()) {
        				switch (type) {
        					case LogType.BEGIN_RECORD:
        						Database.getLogFile().logAbort(tid);
        						return;
        					case LogType.COMMIT_RECORD:
        						throw new IOException("cannot abort a commited transaction");
        					case LogType.ABORT_RECORD:
        						throw new IOException("cannot abort an aborted transaction");
        					case LogType.UPDATE_RECORD:
        						BufferPool bp = Database.getBufferPool();
        						Catalog ctlg = Database.getCatalog();
        						LogFile lf = Database.getLogFile();
        						
        						Page beforeImg = LogFile.readPageData(readOnlyLog);
        						Page afterImg = LogFile.readPageData(readOnlyLog);
        						PageId beforePid = beforeImg.getId();
        						
        						// discard!
        						bp.discardPage(beforePid);
        						
        						// write before image to disk
        						ctlg.getDatabaseFile(beforePid.getTableId()).writePage(beforeImg);
        						
        						// write compensating log record
        						lf.logCLR(tid, beforeImg);
        						
        						break;
        					case LogType.CHECKPOINT_RECORD:
        						break;
        					case LogType.CLR_RECORD:
        						break;
        					default:
        						throw new RuntimeException("Unexpected type!  Type = " + type);
        				} // end switch
        			} // end if
        			
        			currentOffset = recordOffset;
        			
        		} // end while 
        		
        		// move back to end
        		readOnlyLog.seek(readOnlyLog.length());
        		
        	} // end synchronized (this)
        } // end synchronized (Database.getBufferPool())
    } // end rollback(TransactionId)

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
    	synchronized (Database.getBufferPool()) {
    		synchronized (this) {
    			HashSet<Long> losers = new HashSet<Long>();
    			
    			// start from last checkpoint
    			readOnlyLog.seek(0);
    			long lastCheckpoint = readOnlyLog.readLong();
    			
    			if (lastCheckpoint != -1) {	
    				readOnlyLog.seek(lastCheckpoint);
    				readOnlyLog.seek(readOnlyLog.getFilePointer() + 
    						LogFile.INT_SIZE + LogFile.LONG_SIZE);
    				int count = readOnlyLog.readInt();
    				for (int i = 0; i < count; i++){
    					losers.add(readOnlyLog.readLong());
    				}
    				
    				// skip the long at the end
    				readOnlyLog.seek(readOnlyLog.getFilePointer() + 
    						LogFile.LONG_SIZE); 
    			}
    			
    			redo_txns(losers);	// redoes any transaction that is NOT losers
    			undo_txns(losers);
    		}
    	}
    }
    
    
    private void redo_txns(HashSet<Long> losers) throws IOException {
    	while (readOnlyLog.getFilePointer() < readOnlyLog.length()){
			int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
            	case LogType.BEGIN_RECORD:
            		if (losers.contains(tid)) {
            			throw new IOException("already begun");
            		}
            		losers.add(tid);
            		break;
            	case LogType.COMMIT_RECORD:
            		if (!losers.contains(tid))
            			throw new IOException("can't commit, already committed or aborted");
            		losers.remove(tid);
            		break;
            	case LogType.ABORT_RECORD:
            		if (!losers.contains(tid))
            			throw new IOException("can't abort, already committed or aborted");
            		losers.remove(tid);
            		break;
            	case LogType.UPDATE_RECORD:
            		if (!losers.contains(tid))
            			throw new IOException("can't update, already committed or aborted");
            		Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    PageId pid = beforeImg.getId();
                    Database.getBufferPool().discardPage(pid);
                    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(afterImg);
            		break;
            	case LogType.CLR_RECORD:
            		if (!losers.contains(tid))
            			throw new IOException("can't redo CLR, already committed or aborted");
            		afterImg = LogFile.readPageData(readOnlyLog);  // after image
            		pid = afterImg.getId();
            		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(afterImg);
            		break;
            	case LogType.CHECKPOINT_RECORD:
            		throw new RuntimeException("Should not encounter any checkpoint!!");
            	default:
            		throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            
            // skip last long
            readOnlyLog.seek(readOnlyLog.getFilePointer() + LogFile.LONG_SIZE);
		}
    }
    
    
    private void undo_txns(HashSet<Long> losers) throws IOException {
    	readOnlyLog.seek(readOnlyLog.length()); // undoing so move to end of logfile
        
        synchronized (Database.getBufferPool()) {
        	synchronized (this){
        		long current = readOnlyLog.getFilePointer();
        		while (current > LogFile.LONG_SIZE && !losers.isEmpty()) {
	        		readOnlyLog.seek(current - LogFile.LONG_SIZE);
	        		long recordOffset = readOnlyLog.readLong();
	        		readOnlyLog.seek(recordOffset);
	        		
	        		int type = readOnlyLog.readInt();
	                long tid = readOnlyLog.readLong();
	                
	                switch (type) {
	                	case LogType.BEGIN_RECORD:
	                		if (losers.contains(tid)){
 	                			Database.getLogFile().logAbort(tid);
 	                			losers.remove(tid);
 	                		}
 	                		break;
	                	case LogType.COMMIT_RECORD:
	                		if (losers.contains(tid)) {
	                			throw new IOException("Cannot abort a committed transaction!");
	                		}
	                		break;
	                	case LogType.ABORT_RECORD:
	 	                	if (losers.contains(tid)) {
	 	                		throw new IOException("not possible");
	 	                	}
	 	                	break;
	 	                case LogType.UPDATE_RECORD: 
	 	                	if (losers.contains(tid)){
		 	                	Page beforeImg = LogFile.readPageData(readOnlyLog);
		 	                	
		 	                	// read after image! but we don't need it. just skip
		 	                    LogFile.readPageData(readOnlyLog);
		 	                    
		 	                    PageId pid = beforeImg.getId();
		 	                    Database.getBufferPool().discardPage(pid);
		 	                    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(beforeImg);
		 	                    Database.getLogFile().logCLR(tid, beforeImg);
	 	                	}
	 	                    break;
	 	                case LogType.CLR_RECORD:
	 	                	break;
	 	                case LogType.CHECKPOINT_RECORD:
	 	            	   	break;
	 	                default:
	 	                	throw new RuntimeException("Unexpected type!  Type = " + type);
	                }
	                current = recordOffset;
        		}
        	}
        }
    }
}
