import simpledb.metadata.MetadataMgr;
import simpledb.parse.*;
import simpledb.planner.*;
import simpledb.query.*;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class HW9Test {
    public static void main(String[] args) {
        SimpleDB.init("hw9testdb");
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

    private static boolean createTable(String tblname, Transaction tx) {
        MetadataMgr mdmgr = SimpleDB.mdMgr();
        TableInfo ti = mdmgr.getTableInfo(tblname, tx);
        if (ti.recordLength() > 0)
            return false;  // Use existing table
        else {
            String stmt1 = "create table " + tblname + " (A int, B varchar(15), C int)";
            Parser p = new Parser(stmt1);
            CreateTableData data = (CreateTableData) p.updateCmd();
            BasicUpdatePlanner up = new BasicUpdatePlanner();
            up.executeCreateTable(data, tx);
            return true;
        }
    }

    // Create 500 records having A-values from 0 to 499,
    // B-values = A mod 100, and C-values that are either 999 or null.
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
        String stmt = "insert into " + t + "(A, B, C) values (";
        String cval = (i%2 == 0) ? "999" : "null";
        stmt += i + ", 'b" + (i%100) + "', " + cval + ")";
        return stmt;
    }

    private static void query1(String t, Transaction tx) {
        System.out.println("Here are the records having an A-value ending in 77, 88, or 99");
        String qry = "select A,B,C from " + t + " where B = 'b77' "
                + "union select A,B,C from " + t + " where b = 'b88' "
                + "union select A,B,C from " + t + " where b = 'b99' ";
        Planner plnr = SimpleDB.planner();
        Plan plan = plnr.createQueryPlan(qry, tx);
        Scan s = plan.open();
        while (s.next())
            System.out.println(getInt(s, "a") + "\t" + getString(s, "b") + "\t" + getInt(s, "c"));
        s.close();
    }

    private static void query2(String t, Transaction tx) {
        System.out.println("Here are renamed records having an A-value < 20 and a C-value of null");
        String qry = "select A as X, B as Y, C as Z from " + t + " where C is null and A < 20";
        Planner plnr = SimpleDB.planner();
        Plan plan = plnr.createQueryPlan(qry, tx);
        Scan s = plan.open();
        while (s.next())
            System.out.println(getInt(s, "x") + "\t" + getString(s, "y") + "\t" + getInt(s, "z"));
        s.close();
    }

    private static String getInt(Scan s, String fldname) {
        //if (s.isNull(fldname))
        //    return "null";
//        else
            return "" + s.getInt(fldname);
    }

    private static String getString(Scan s, String fldname) {
//        if (s.isNull(fldname))
//            return "null";
//        else
            return "" + s.getString(fldname);
    }
}
