package simpledb;

import java.io.File;
import java.util.ArrayList;

public class Lab2Main {

	public static void main(String[] args) {
		// set up
		File f = new File("some_data_file.dat");
		Type[] typeArr = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
		String[] fieldArr = {"field0", "field1", "field2"};
		TupleDesc td = new TupleDesc(typeArr, fieldArr);
		HeapFile hf = new HeapFile(f, td);
		Database.getCatalog().addTable(hf);
		
		
		// get tuples to update
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		DbFileIterator hfIter = hf.iterator(new TransactionId());
		try {
			hfIter.open();
			while (hfIter.hasNext()) {
				Tuple curr = hfIter.next();
				if (curr.getField(1).compare(Predicate.Op.LESS_THAN, new IntField(3))) {
					tuples.add(curr);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("could not get tuples to update");
		}
		
		// update tuples
		try {
			for (Tuple t : tuples) {
				Tuple tToAdd = new Tuple(td);
				tToAdd.setField(0, t.getField(0));
				tToAdd.setField(1, new IntField(3));
				tToAdd.setField(2, t.getField(2));
				
				Database.getBufferPool().deleteTuple(new TransactionId(), t);
				Database.getBufferPool().insertTuple(new TransactionId(), hf.getId(), tToAdd);
				
				System.out.println("Update tuple: " + t + "to be: " + tToAdd);
			}
		} catch (Exception e) {
			throw new RuntimeException("could not update tuples");
		}
		
		// add tuple (99, 99, 99)
		Tuple newTuple = new Tuple(td);
		for (int i = 0; i < 3; i++) {
			newTuple.setField(i, new IntField(99));
		}
		try {
			Database.getBufferPool().insertTuple(new TransactionId(), hf.getId(), newTuple);
			System.out.println("Insert tuple: " + newTuple);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("could not add tuple (99,99,99)");
		}
		
		// scan table and print out all tuples
		System.out.println("The table now contains the following records:");
		try {
			hfIter.rewind();
			int row = 0;
			while (hfIter.hasNext()) {
				Tuple curr = hfIter.next();
				System.out.println("Row " + (row++) + ": " + curr);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("could not scan table/print tuples");
		}
		
		// flush all changes to disk
		try {
			Database.getBufferPool().flushAllPages();
			hfIter.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("could not flush changes to disk");
		}
	}
}
