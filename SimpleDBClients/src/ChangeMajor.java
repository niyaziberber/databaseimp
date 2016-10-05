import java.sql.*;
import simpledb.remote.SimpleDriver;

public class ChangeMajor {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			for (int i = 0; i <10; i++) {
				Statement stmt = conn.createStatement();

				String cmd = "update STUDENT set MajorId=30 "
						+ "where SName = 'amy'";
				stmt.executeUpdate(cmd);
				System.out.println("Amy is now a drama major.");
				//Thread.sleep(200);
			}
		}
		catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
