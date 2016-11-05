package hw1;

import simpledb.remote.SimpleDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.*;

public class StudentGPA {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            Reader rdr = new InputStreamReader(System.in);
            BufferedReader br = new BufferedReader(rdr);

            while (true) {
                System.out.print("Name: ");
                String name = br.readLine().trim();
                if (name.startsWith("exit"))
                    break;
                String s = "select sid from STUDENT where sname ='" + name + "'";
                ResultSet rs = stmt.executeQuery(s);
                boolean studentExist = rs.next();
                rs.close();
                if (!studentExist) {
                    System.out.println("No such student.");
                } else {
                    s = "select Points from STUDENT, ENROLL, GRADEPOINTS where sid = StudentId and Grade = LetterGrade and sname ='" + name + "'";
                    rs = stmt.executeQuery(s);

                    if (!rs.next()) {
                        System.out.println("No courses taken.");
                    } else {
                        int totalgp = 0;
                        int courseCount = 0;
                        do {
                            int gp = rs.getInt("Points");
                            totalgp += gp;
                            courseCount++;
                        } while (rs.next());
                        rs.close();
                        float gpa = totalgp / 10.0f / courseCount;
                        System.out.println("GPA is: " + gpa);
                    }
                }
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
