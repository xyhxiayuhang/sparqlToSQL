import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class connectDatabase {

	public Connection connect(String url, String user, String password) throws SQLException {
		Connection connection = null;
		String driver = "com.mysql.jdbc.Driver";
		try {
			Class.forName(driver);
			connection = DriverManager.getConnection(url, user, password);
			if (!connection.isClosed())
				System.out.println("Succeeded connecting to the Database!");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return connection;

	}
}
