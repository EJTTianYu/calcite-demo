import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

public class CalciteSQLServerConnection {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {

    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // 本地Schema.
    // Schema schema = ReflectiveSchema.create(calciteConnection, rootSchema, "hr",
    // new HrSchema());

    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:sqlserver://");
    dataSource.setUsername("root");
    dataSource.setPassword("123456");
    Schema schema = JdbcSchema.create(rootSchema, "ex", dataSource, null, "sql_learn");

    rootSchema.add("ex", schema);
//    System.out.println(calciteConnection.getMetaData());
    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
//    ResultSet rs = databaseMetaData.getTables(null, null, "%", new String[]{"TABLE"});
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select * from ex.primary_test");

    output(resultSet, System.out);
    resultSet.close();
    statement.close();
    connection.close();
  }

  private static void output(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    out.println(metaData.getColumnLabel(1) + " " + metaData.getColumnLabel(2));
    out.println(metaData.getColumnTypeName(1) + " " + metaData.getColumnTypeName(2));
    while (resultSet.next()) {
      for (int i = 1; ; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }
}



