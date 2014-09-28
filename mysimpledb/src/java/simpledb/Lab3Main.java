package simpledb;

import java.io.IOException;

public class Lab3Main {
	public static void main(String[] argv) 
			throws DbException, TransactionAbortedException, IOException {

		System.out.println("Loading schema from file:");
		// file named college.schema must be in mysimpledb directory
		Database.getCatalog().loadSchema("college.schema");

		// SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
		// algebra translation: select_{name="alice"}( Students )
		// query plan: a tree with the following structure
		// - a Filter operator is the root; filter keeps only those w/ name=Alice
		// - a SeqScan operator on Students at the child of root
		TransactionId tid = new TransactionId();
		SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
		StringField alice = new StringField("alice", Type.STRING_LEN);
		Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
		Filter filterStudents = new Filter(p, scanStudents);

		// query execution: we open the iterator of the root and iterate through results
		System.out.println("Query results:");
		filterStudents.open();
		while (filterStudents.hasNext()) {
			Tuple tup = filterStudents.next();
			System.out.println("\t" + tup);
		}
		filterStudents.close();
		Database.getBufferPool().transactionComplete(tid);
	}

}

