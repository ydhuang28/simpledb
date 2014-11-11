package simpledb;

import java.io.Serializable;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	/** Serialization for concurrency. */
    private static final long serialVersionUID = 1L;
    
    /** Schema corresponding to the tuple. */
    private TupleDesc td;
    
    /** Record ID for the tuple. */
    private RecordId rid;
    
    /** An array of fields in the tuple. */
    private Field[] fieldAr;

    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	if (td == null) throw new NullPointerException("null TupleDesc");
    	
        this.td = td;
        this.rid = null;
        this.fieldAr = new Field[td.numFields()];
    } // end Tuple(TupleDesc)

    
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return td;
    } // end getTupleDesc()

    
    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return rid;
    } // end getRecordId()

    
    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    } // end setRecordId(RecordId)

    
    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= fieldAr.length) {
        	throw new RuntimeException("invalid index");
        } else if (f == null) {
        	throw new NullPointerException("field given is null");
        }
        
        Type ithFieldType = td.getFieldType(i);
        if (!ithFieldType.equals(f.getType())) {
        	throw new RuntimeException("types do not agree");
        }
        
        fieldAr[i] = f;
    } // end setField(int, Field)

    
    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
    	if (i < 0 || i >= fieldAr.length) {
        	throw new RuntimeException("invalid index");
        }
    	
    	return fieldAr[i];
    } // end getField(int)

    
    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
        String str = "";
        for (int i = 0; i < fieldAr.length; i++) {
        	if (Type.INT_TYPE.equals(fieldAr[i].getType())) {
        		if (fieldAr[i] != null) {
        			str += ((IntField) fieldAr[i]).getValue() + " ";
        		} else str += "   ";
        		
        	} else {
        		if (fieldAr[i] != null) {
        			str += ((StringField) fieldAr[i]).getValue() + " ";
        		} else str += "   ";
        	}
        }
        
        return str;
    } // end toString()

} // end Tuple
