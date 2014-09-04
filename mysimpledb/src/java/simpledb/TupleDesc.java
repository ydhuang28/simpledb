package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field.
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field.
         */
        public final Type fieldType;

        /**
         * The name of the field.
         */
        public final String fieldName;

        /**
         * Constructor.
         * 
         * @param t type of this TDItem
         * @param n name of this TDItem
         */
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        /** 
         * Returns a description of this TDItem.
         */
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;
    
    /** Array for types of field. */
    private TDItem[] itemAr;
    
    /** Size (in bytes) of this TupleDesc. */
    private int size;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	// entry checking
        if (typeAr.length < 1) {
        	throw new RuntimeException("need at least one type");
        } else if (typeAr.length != fieldAr.length) {
        	throw new RuntimeException("arrays not of same length");
        }
        
        // initialize
        itemAr = new TDItem[typeAr.length];
        size = 0;
        for (int i = 0; i < itemAr.length; i++) {
        	// calculate size
        	size += typeAr[i].getLen();
        	
        	if (fieldAr[i] == null) {
        		itemAr[i] = new TDItem(typeAr[i], "");
        	} else {
        		itemAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        	}
        }
        
    } // end TupleDesc(Type[], String[])

    
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    } // end TupleDesc(Type[])
    
    
    /**
     * Private constructor used for the merge operation.
     */
    private TupleDesc(TDItem[] itemAr) {
    	this.itemAr = new TDItem[itemAr.length];
    	System.arraycopy(itemAr, 0, this.itemAr, 0, itemAr.length);
    	
    	// calculate size
    	size = 0;
    	for (TDItem item : itemAr) {
    		size += item.fieldType.getLen();
    	}
    	
    } // end TupleDesc(TDItem[])
    
    
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return itemAr.length;
    } // end numFields()

    
    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= itemAr.length || i < 0) {
        	throw new NoSuchElementException("invalid index");
        }
        return itemAr[i].fieldName;
    } // end getFieldName(int)

    
    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i >= itemAr.length || i < 0) {
        	throw new NoSuchElementException("invalid index");
        }
        return itemAr[i].fieldType;
    } // end getFieldType(int)

    
    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
        	throw new NoSuchElementException("given name is null");
        }
    	
    	for (int i = 0; i < itemAr.length; i++) {
        	if (name.equals(itemAr[i].fieldName)) {
        		return i;
        	}
        }
        
    	throw new NoSuchElementException("no field with name is found");
    } // end fieldNameToIndex(String)

    
    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    } // end getSize()

    
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	TDItem[] merged = new TDItem[td1.itemAr.length + td2.itemAr.length];
    	System.arraycopy(td1.itemAr, 0, merged, 0, td1.itemAr.length);
    	System.arraycopy(td2.itemAr, 0, merged,
    					 td1.itemAr.length, td2.itemAr.length);
    	
        return new TupleDesc(merged);
    } // end merge(TupleDesc, TupleDesc)

    
    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object td) {
    	if (td == null) return false;
    	else if (!td.getClass().equals(this.getClass())) {
        	return false;
        }
        
        TupleDesc other = (TupleDesc) td;
        if (other.itemAr.length != this.itemAr.length) {
        	return false;
        }
        
        for (int i = 0; i < itemAr.length; i++) {
        	if (!this.itemAr[i].fieldType.equals(other.itemAr[i].fieldType)) {
        		return false;
        	}
        }
        
        return true;
    } // end equals(Object)

    
    /**
     * Not implemented yet.
     * 
     * @return hash code for this TupleDesc.
     */
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    } // end hashCode()

    
    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String string = "";
        for (int i = 0; i < itemAr.length; i++) {
        	if (i != itemAr.length-1) {
        		string += itemAr[i] + ", ";
        	} else {
        		string += itemAr[i];
        	}
        }
        return string;
    } // end toString()

    
    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        List<TDItem> itemList = Arrays.asList(itemAr);
        return itemList.iterator();
    } // end iterator()

} // end TupleDesc
