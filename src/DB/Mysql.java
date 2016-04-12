package DB;

import java.sql.*;

public class Mysql {

	private String connectUrl;
	private Connection con = null;
	private Statement stmt = null;
	
	public Mysql(String username){
		connectUrl = "jdbc:mysql://localhost/test?"+"user="+username;
	}
	
	public Mysql(String username, String password){
		connectUrl = "jdbc:mysql://localhost/test?"+"user="+username+"&password"+password;
	}
	
	public void initConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch(java.lang.ClassNotFoundException e) {
			System.err.print("ClassNotFoundException: "); 
			System.err.println(e.getMessage());
		}
		try {
			con = DriverManager.getConnection(connectUrl);
		} catch(SQLException ex) {
			System.err.println("SQLException: " + ex.getMessage());
		}
	}
	
	/**
	 * 外部获取stmt来执行
	 * Mysql db = new Mysql("root");
	 * try {
	 *		db.initConnection();
	 *		db.getStmt().executeUpdate(createString);
	 * } catch(SQLException ex){
	 *		System.err.println("SQLException: " + ex.getMessage());
	 * } finally {
	 *		db.close();
	 * }
	 * 
	 * @return
	 */
	public Statement getStmt(){
		if (con == null){
			System.err.println("connection has not been initialiazed, return null");
			return null;
		}
		try {
			stmt = con.createStatement();
			return stmt;
		} catch (SQLException ex){
			System.err.println("SQLException: " + ex.getMessage());
		}
		return null;
	}
	
	public void close(){
		try {
			if (stmt != null){
				stmt.close();
			}
			if (con != null){
				con.close();
			}
		} catch (SQLException ex){
			System.err.println("SQLException: " + ex.getMessage());
		}
	}
	
}
