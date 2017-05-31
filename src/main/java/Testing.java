import util.SimpleUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;


public class Testing 
{
	
	
	public static void main(String args[])
	{
	Connection conn = null;

	try {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    conn =
	       DriverManager.getConnection("jdbc:mysql://localhost/LOGGING?" +
	                                   "user=root&password=reeta");

	    // Do something with the Connection


	} catch (SQLException ex) {
	    // handle any errors
	    System.out.println("SQLException: " + ex.getMessage());
	    System.out.println("SQLState: " + ex.getSQLState());
	    System.out.println("VendorError: " + ex.getErrorCode());
	}
	}

}
