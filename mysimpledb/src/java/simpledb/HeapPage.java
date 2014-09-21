package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte) 0);
    
    private boolean dirty;
    private TransactionId lastTrnsctnToDirty;
    
    private int numEmptySlots;

    
    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p/>
     * ceiling(no. tuple slots / 8)
     * <p/>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.dirty = false;
        this.lastTrnsctnToDirty = null;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        this.numEmptySlots = numSlots;
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
            	numEmptySlots--;
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    } // end HeapPage(HeapPageId, byte[])

    
    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return (int) Math.floor((BufferPool.PAGE_SIZE * 8) /
        					 	(td.getSize() * 8 + 1));
    } // end getNumTuples()

    
    /**
     * Computes the number of bytes in the header of a page in a HeapFile with
     * each tuple occupying tupleSize bytes.
     *
     * @return the number of bytes in the header of a page in a HeapFile
     * 		   with each tuple occupying tupleSize bytes.
     */
    private int getHeaderSize() {
        return (int) (Math.ceil(numSlots / 8.0));
    } // end getHeaderSize()

    
    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    } // end getBeforeImage()

    
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    } // end setBeforeImage()

    
    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    } // end getId()

    
    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    } // end readNextTuple(DataInputStream, int)

    
    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p/>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() -
        		(header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    } // end getPageData()

    
    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned byte array.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    } // end createEmptyPageData()

    
    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
    	// argument checking
        if (!pid.equals(t.getRecordId().getPageId())) {
        	throw new DbException("tuple not on this page");
        } else if (!isSlotUsed(t.getRecordId().tupleno())) {
        	throw new DbException("tuple slot already empty");
        }

        markSlotUsed(t.getRecordId().tupleno(), false);	// mark not used
    } // end deleteTuple(Tuple)

    
    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
    	// argument checking
        if (getNumEmptySlots() == 0) {
        	throw new DbException("no empty slots");
        } else if (!td.equals(t.getTupleDesc())) {
        	throw new DbException("TupleDesc mismatch");
        }
        
        int insertPos = 0;								// find empty spot
        while (isSlotUsed(insertPos++));				// increment until not used
        
        tuples[insertPos] = t;							// insert tuple
        markSlotUsed(insertPos, true);					// mark slot used
        
        t.setRecordId(new RecordId(pid, insertPos));	// set record id correctly
    } // end insertTuple(Tuple)

    
    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        lastTrnsctnToDirty = dirty ? tid : null;
    } // end markDirty(boolean, TransactionId)

    
    /**
     * Returns the tid of the transaction that last dirtied this page,
     * or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return lastTrnsctnToDirty;     
    } // end isDirty()

    
    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        return numEmptySlots;
    } // end getNumEmptySlots()
    
    
    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        if (i < 0 || i >= numSlots) return false;
        
        int bitPos = i % Byte.SIZE;
    	byte mask = (byte) (1 << bitPos);
        
        return ((header[i / Byte.SIZE] & mask) >> bitPos) == 1;
    } // end isSlotUsed(int)

    
    /**
     * Abstraction to fill or clear a slot on this page.
     * 
     * method:
     *   if true, create mask such that 1 is on the bit
     *     position and 0 everywhere else, and then bitwise-OR the mask
     *     with the header to get the new header.
     *   otherwise, create mask such that 1 is on every bit except
     *     for the bit position for tuple index i, which has 0, and
     *     bitwise-AND the mask with the header to get the new header.
     */
    private void markSlotUsed(int i, boolean value) {
    	// calculate bit position
    	int bitPos = 1 << (i % Byte.SIZE);
    	
    	// create mask depending on value
    	byte mask = (byte) (value ? bitPos : (Math.pow(2, Byte.SIZE) - 1) - bitPos);
    	
    	// bitwise-OR or AND depending on value
    	if (value) {
    		header[i / Byte.SIZE] = (byte) (header[i / Byte.SIZE] | mask);
    		numEmptySlots--;
    	} else {
    		header[i / Byte.SIZE] = (byte) (header[i / Byte.SIZE] & mask);
    		numEmptySlots++;
    	}
    } // end markSlotUsed(int, boolean)

    
    /**
     * @return an iterator over all tuples on this page
     * (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        List<Tuple> tupleList = new ArrayList<Tuple>();
        for (Tuple t : tuples) {
        	if (t != null) tupleList.add(t);
        }
        return tupleList.iterator();
    } // end iterator()

} // end HeapPage

