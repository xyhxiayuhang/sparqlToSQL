import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class main {

	public static void main(String[] args) throws IOException, SQLException {
		// connect to MySQL database
		String url = "jdbc:mysql://172.20.46.89:3306/xiayuhang5?characterEncoding=UTF-8";
		String user = "xiayuhang";
		String password = "hadoop";
		Connection connection = new connectDatabase().connect(url, user, password);

		String SPARQLPath = "SPARQL.txt";
		String SQLPath = "sql.txt";

		sparqlToSql sToSql = new sparqlToSql();
		sToSql.transform(SPARQLPath, SQLPath, connection);
	}
}
