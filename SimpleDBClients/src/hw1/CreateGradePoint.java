package hw1;

import simpledb.remote.SimpleConnection;
import simpledb.remote.SimpleDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateGradePoint {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            String s = "create table GRADEPOINTS (LetterGrade varchar(2), Points int)";
            stmt.executeUpdate(s);
            System.out.println("Table GRADEPOINT created.");

            s = "insert into GRADEPOINTS(LetterGrade, Points) values ";
            String[] vals = {   "('A', 40)",
                    "('A-', 37)",
                    "('B+', 33)",
                    "('B',  30)",
                    "('B-', 27)",
                    "('C+', 24)",
                    "('C', 20)",
                    "('C-', 17)",
                    "('D+', 14)",
                    "('D', 10)",
                    "('D-', 7)",
                    "('F', 0)" };
            for (String val : vals)
                stmt.executeUpdate(s + val);
            System.out.println("Table GRADEPOINT populated.");

        } catch(SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
