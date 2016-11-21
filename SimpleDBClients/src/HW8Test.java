import simpledb.metadata.MetadataMgr;
import simpledb.parse.*;
import simpledb.planner.*;
import simpledb.query.*;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;


public class HW8Test {
	public static void main(String[] args) {
		SimpleDB.init("hw8testdb");
		String tblname = "t";
		Transaction tx = new Transaction();
		boolean b = createTable(tblname, tx);
		if (b)
			populateTable(tblname, tx);
		tx.commit();
		tx = new Transaction();
		query1(tblname, tx);
		query2(tblname, tx);
		tx.commit();
	}

	// Note that the parser saves all table names and field names in lower case.
	// So the test code needs to refer to fields in lower case.

	private static boolean createTable(String tblname, Transaction tx) {
		MetadataMgr mdmgr = SimpleDB.mdMgr();
		TableInfo ti = mdmgr.getTableInfo(tblname, tx);
		if (ti.recordLength() > 0)
			return false;  // Use existing table
		else {
			String stmt1 = "create table " + tblname + " (A int, B varchar(15))";
			Parser p = new Parser(stmt1);
			CreateTableData data = (CreateTableData) p.updateCmd();
			BasicUpdatePlanner up = new BasicUpdatePlanner();
			up.executeCreateTable(data, tx);
			return true;
		}
	}

	// Create 500 records having A-values from 1 to 500.
	// Every even record will have a B-value of null.
	private static void populateTable(String tblname, Transaction tx) {
		for (int i=0; i<500; i++) {
			String stmt = makeInsertStmt(tblname, i);
			Parser p = new Parser(stmt);
			InsertData data = (InsertData) p.updateCmd();
			BasicUpdatePlanner up = new BasicUpdatePlanner();
			up.executeInsert(data, tx);
		}
	}

	private static String makeInsertStmt(String t, int i) {
		String stmt = "insert into " + t + "(A, B) values (";
		stmt += i + ", ";
		if (i%2 == 0)
			stmt += "null";
		else
			stmt += "'b" + (i%20) + "'";
		return stmt + ")";
	}

	private static void query1(String tblname, Transaction tx) {
		Plan p1 = new TablePlan(tblname, tx);

		//A predicate corresponding to "A<100 and A>50".
		Expression lhs = new FieldNameExpression("a");  //lower case "a", not "A"!
		Constant c1 = new IntConstant(100);
		Expression rhs1 = new ConstantExpression(c1);
		Term t1 = new Term(lhs, "<", rhs1);
		Constant c2 = new IntConstant(50);
		Expression rhs2 = new ConstantExpression(c2);
		Term t2 = new Term(lhs, ">", rhs2);
		Predicate selectpred = new Predicate(t1);
		Predicate pred2 = new Predicate(t2);
		selectpred.conjoinWith(pred2);

		Plan p2 = new SelectPlan(p1, selectpred);
		print(p2, "Here are the records between 51 and 99:");
	}

	private static void print(Plan p, String msg) {
		System.out.println(msg);
		Scan s = p.open();
		while (s.next()) {
			int a = s.getInt("a");  //lower case!
			String b;
			Constant bconst = s.getVal("b");  //lower case!
			if (bconst instanceof NullConstant)
				b = "null";
			else
				b = (String) bconst.asJavaVal();
			System.out.println(a + " " + b);
		}
		s.close();
		System.out.println();
	}

	private static void query2(String t, Transaction tx) {
		System.out.println("Here are the A-values <30 of records having a null B-value");
		// This query should return the even numbers from 0 to 28
		// Note the query can use upper case field names because
		// the parser converts them to lower case
		String qry = "select A from " + t + " where B is null and A < 30";
		Parser p = new Parser(qry);
		QueryData data = p.query();
		QueryPlanner qp = new BasicQueryPlanner();
		Plan plan = qp.createPlan(data, tx);
		Scan s = plan.open();
		while (s.next())
			System.out.println(s.getInt("a"));  // lower case!
		s.close();
	}
}
