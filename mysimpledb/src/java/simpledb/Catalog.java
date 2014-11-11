package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
	
	/** Map to store the tables; names as keys. */
	private Map<String, DbFile> tableMap;
	
	/** Map to store the name of the primary fields of the tables, if any. */
	private Map<String, String> pFieldTableMap;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tableMap = new HashMap<String, DbFile>();
        pFieldTableMap = new HashMap<String, String>();
    } // end Catalog()

    
    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identifier of
     *                  this file/tupledesc parameter for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.
     * 					If a name conflict exists, use the last table to be added as the table
     * 					for a given name.
     * @param pkeyField the name of the primary key field
     *                  
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if (name == null || file == null)
        	throw new NullPointerException("null parameter(s)");
        
        // remember to remove previous entries for same file!!
        if (tableMap.containsValue(file)) {
        	ArrayList<String> toRemove = new ArrayList<String>();
        	for (Map.Entry<String, DbFile> e : tableMap.entrySet()) {
        		if (e.getValue().equals(file)) {
        			toRemove.add(e.getKey());
        		}
        	}
        	for (String s : toRemove) {
        		tableMap.remove(s);
        	}
        }
        
        // put new entry
        tableMap.put(name, file);
        
        // set pkeyField if any
        if (pkeyField != null) {
        	if (!"".equals(pkeyField)) {
        		pFieldTableMap.put(name, pkeyField);
        	}
        }
    } // end addTable(DbFile, String, String)

    
    /**
     * Add a new table without a primary key field.
     * 
     * @param file the contents of the table to add;  file.getId() is the identifier of
     *             this file/tupledesc parameter for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.
     * 			   If a name conflict exists, use the last table to be added as the
     * 			   table for a given name.
     */
    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    } // end addTable(DbFile, String)

    
    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    } // end addTable(DbFile)

    
    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (!tableMap.containsKey(name)) 
        	throw new NoSuchElementException("table with specified name DNE");
        
        return tableMap.get(name).getId();
    } // end getTableId(String)

    
    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        for (DbFile t : tableMap.values()) {
        	if (t.getId() == tableid) {
        		return t.getTupleDesc();
        	}
        }
        
        throw new NoSuchElementException("table with specified table id DNE");
    } // end getTupleDesc(int)

    
    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for (DbFile t : tableMap.values()) {
        	if (t.getId() == tableid) {
        		return t;
        	}
        }
        
        throw new NoSuchElementException("table with specified table id DNE");
    } // end getDatabaseFile(int)

    
    /**
     * Returns the primary key field name of the table associated
     * with the table id provided, if one exists; otherwise returns
     * an empty string.
     * 
     * @param tableid id of the table associated with
     * @return primary key if exists; empty string if not
     */
    public String getPrimaryKey(int tableid) {
    	String keyName = "";
    	for (String name : tableMap.keySet()) {
        	if (tableMap.get(name).getId() == tableid) {
        		if (pFieldTableMap.containsKey(name)) {
        			keyName = pFieldTableMap.get(name);
        		}
        	}
        }
    	
    	return keyName;
    } // end getPrimaryKey(int)

    
    /**
     * @return an iterator over the table ids.
     */
    public Iterator<Integer> tableIdIterator() {
        List<Integer> tableIdList = new ArrayList<Integer>();
        for (DbFile t : tableMap.values()) {
        	tableIdList.add(t.getId());
        }
        
        return tableIdList.iterator();
    } // end tableIdIterator()

    
    /**
     * Returns the name of table associated with the given id.
     * 
     * @param tableid id of table associated with
     * @return name of table if exists; null if not
     */
    public String getTableName(int tableid) {
    	for (String name : tableMap.keySet()) {
        	if (tableMap.get(name).getId() == tableid) {
        		return name;
        	}
        }
    	
        return null;
    } // end getTableName(int)

    
    /**
     * Delete all tables from the catalog.
     */
    public void clear() {
        tableMap.clear();
        pFieldTableMap.clear();
    } // end clear()

    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t + (primaryKey.equals("")? "":(" key is " + primaryKey)));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    } // end loadSchema(String)
    
} // end Catalog

