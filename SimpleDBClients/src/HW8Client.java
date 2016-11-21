
import simpledb.metadata.MetadataMgr;
import simpledb.parse.CreateTableData;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.planner.BasicQueryPlanner;
import simpledb.planner.BasicUpdatePlanner;
import simpledb.planner.QueryPlanner;
import simpledb.query.Constant;
import simpledb.query.ConstantExpression;
import simpledb.query.Expression;
import simpledb.query.FieldNameExpression;
import simpledb.query.IntConstant;
import simpledb.query.NullConstant;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.query.Term;
import simpledb.record.TableInfo;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.*;

public class HW8Client {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();
            String updateAmy = updateAmy();
            stmt.executeUpdate(updateAmy);
            String s = "select sname, gradyear from student";
            ResultSet rs = stmt.executeQuery(s);
            System.out.println("After updating Amy");
            while (rs.next())
                System.out.println(rs.getString("SName") + "\t" + rs.getInt("GradYear"));
            rs.close();
            System.out.println();
            String putNewRec = insertNewStudentRecord();
            stmt.executeUpdate(putNewRec);
            rs = stmt.executeQuery(s);
            System.out.println("After adding Tom");
            while (rs.next())
                System.out.println(rs.getString("SName") + "\t" + rs.getInt("GradYear"));
            rs.close();
            System.out.println();
            String printAllStudents = "select SName, GradYear from STUDENT where GradYear > 2001 and GradYear < 2005";
            rs = stmt.executeQuery(printAllStudents);
            System.out.println("Printing all graduates between '01 and '05");
            while (rs.next()) {
                System.out.println(rs.getString("Sname") + "\t" + rs.getInt("GradYear"));
            }
            rs.close();
            System.out.println();
            rs = stmt.executeQuery(printNull());
            System.out.println("Printing all null grad years");
            while (rs.next()) {
                String sname = rs.getString("SName");
                System.out.println(sname);
            }
            rs.close();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            }

            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Note that the parser saves all table names and field names in lower case.
    // So the test code needs to refer to fields in lower case.

    private static String updateAmy() {
        return "update STUDENT set GradYear=null where SName = 'amy'";
    }

    private static String insertNewStudentRecord() {
        return "insert into STUDENT(SId, SName, MajorId, GradYear) values " +
                "(10, 'tom', 20, null)";
    }

    private static String printNull() {
        return "select sname from student where gradyear is null";
    }
}
