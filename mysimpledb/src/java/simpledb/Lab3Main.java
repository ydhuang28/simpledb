package simpledb;

import java.io.IOException;
import java.util.ArrayList;

public class Lab3Main {
	
	public static void main(String[] argv) 
			throws DbException, TransactionAbortedException, IOException {

		// -- exercise 3 --
		
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
		System.out.println("Exercise 3, query 1 results:");
		filterStudents.open();
		while (filterStudents.hasNext()) {
			Tuple tup = filterStudents.next();
			System.out.println("\t" + tup);
		}
		filterStudents.close();
		Database.getBufferPool().transactionComplete(tid);
		
		// -- exercise 5 --
		
		// SQL query: select *
		//			  from Profs P, Courses C
		//			  where P.favoriteCourse = C.cID;
		// algebra translation: join_{P.favoriteCourse = C.cID}
		//								(Profs P, Courses C)
		// query plan: Join operator with JoinPredicate
		//								P.favoriteCourse = C.cID
		tid = new TransactionId();
		SeqScan scanProfs = new SeqScan(tid,
				Database.getCatalog().getTableId("Profs"));
		SeqScan scanCourses = new SeqScan(tid,
				Database.getCatalog().getTableId("Courses"));
		JoinPredicate jp = new JoinPredicate(2, Predicate.Op.EQUALS, 0);
		Join j = new Join(jp, scanProfs, scanCourses);
		
		// query 2 execution: open join and iterate
		System.out.println("Exercise 5i, query 2 results:");
		j.open();
		for (Tuple t = j.fetchNext(); t != null; t = j.fetchNext()) {
			System.out.println("\t" + t);
		}
		j.close();
		Database.getBufferPool().transactionComplete(tid);
		
		// SQL query: select * from Students S, Takes T where S.sID = T.sID;
		// algebra translation: join_{S.sID = T.sID}(Students S, Takes T)
		// query plan: Join operator with JoinPredicate S.sID = T.sID
		tid = new TransactionId();
		scanStudents = new SeqScan(tid,
				Database.getCatalog().getTableId("Students"));
		SeqScan scanTakes = new SeqScan(tid,
				Database.getCatalog().getTableId("Takes"));
		jp = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
		j = new Join(jp, scanStudents, scanTakes);
		
		// query 3a (students outside) execution: open join and iterate
		System.out.println("Exercise 5ii, query 3a " 
							+ "(Students outside) results:");
		j.open();
		for (Tuple t = j.fetchNext(); t != null; t = j.fetchNext()) {
			System.out.println("\t" + t);
		}
		j.close();
		Database.getBufferPool().transactionComplete(tid);
		
		// query 3b (students inside) execution: open join and iterate
		tid = new TransactionId();
		scanStudents.open();
		scanTakes.open();
		scanStudents.rewind();
		scanTakes.rewind();
		j = new Join(jp, scanTakes, scanStudents);
		System.out.println("Exercise 5ii, query 3b " 
						   + "(Students inside) results:");
		j.open();
		for (Tuple t = j.fetchNext(); t != null; t = j.fetchNext()) {
			System.out.println("\t" + t);
		}
		j.close();
		Database.getBufferPool().transactionComplete(tid);
		
		// -- exercise 7 --
		
		// query 4 execution
		tid = new TransactionId();
		scanStudents.open();
		scanTakes.open();
		scanStudents.rewind();
		scanTakes.rewind();
		scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));
		scanProfs.open();
		JoinPredicate sJoinTPred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
		Join sJoinT = new Join(sJoinTPred, scanStudents, scanTakes);
		JoinPredicate sJoinTjoinPPred = new JoinPredicate(3, Predicate.Op.EQUALS, 2);
		Join sJoinTjoinP = new Join(sJoinTjoinPPred, sJoinT, scanProfs);
		Predicate overallPred = new Predicate(5, Predicate.Op.EQUALS, new StringField("hay", 50));
		Filter overallFilter = new Filter(overallPred, sJoinTjoinP);
		ArrayList<Integer> fieldList = new ArrayList<Integer>();
		fieldList.add(1);
		Type[] typeAr = {Type.STRING_TYPE};
		Project proj = new Project(fieldList, typeAr, overallFilter);
		System.out.println("Exercise 7, query 4 results:");
		proj.open();
		for (Tuple t = proj.fetchNext(); t != null; t = proj.fetchNext()) {
			System.out.println("\t" + t);
		}
		proj.close();
		Database.getBufferPool().transactionComplete(tid);
	} // end main(String[])

} // end Lab3Main

