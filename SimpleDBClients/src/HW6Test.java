import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class HW6Test {
	public static void main(String[] args) {
		SimpleDB.initFileLogAndBufferMgr("hw6testdb");
		Transaction tx = new Transaction();

		Schema sch = new Schema();
		sch.addIntField("A");
		sch.addStringField("B", 50);
		TableInfo ti = new TableInfo("testfile", sch);
		RecordFile rf = new RecordFile(ti, tx);
		rf.beforeFirst();

		if (SimpleDB.fileMgr().size(ti.fileName()) == 1) {
			System.out.println("Populating new table");
			populateTable(rf);
		}
		else
			System.out.println("Using existing table");

		System.out.println("Here are the first 10 records");
		rf.beforeFirst();
		for (int i=0; i<10; i++) {
			rf.next();
			printCurrentRecord(rf);
		}

		System.out.println("\nHere are the 5 recent records backwards");
		for (int i=0; i<5; i++) {
			rf.previous();
			printCurrentRecord(rf);
		}
	
		System.out.println("\nHere is the first record again");
		while(rf.previous())
			; // do nothing
		rf.next();
		printCurrentRecord(rf);

	
		System.out.println("\nAnd here are the last 5 records");
		rf.afterLast();
		for (int i=0; i<5; i++)
			rf.previous();
		for (int i=0; i<5; i++) {
			printCurrentRecord(rf);
			rf.next();
		}
		
		rf.close();
		tx.commit();
	}

	// Create 500 records having A-values from 1 to 500.
	// Every even record will have a B-value of null.
	private static void populateTable(RecordFile rf) {
		for (int i=1; i<=500; i++) {
			rf.insert();
			rf.setInt("A", i);
			if (i%2 == 0)
				rf.setNull("B");
			else
				rf.setString("B", "record"+i);
		}
	}

	private static void printCurrentRecord(RecordFile rf) {
		int aval = rf.getInt("A");
		String bval = rf.isNull("B") ? "null" : rf.getString("B");
		System.out.println(aval + "\t" + bval);
	}
}