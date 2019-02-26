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

public class CalciteMysqlConnectionIns {

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

    Class.forName("com.mysql.jdbc.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://192.168.130.7/hr");
    dataSource.setUsername("root");
    dataSource.setPassword("123456");
    Schema schema = JdbcSchema.create(rootSchema, "ex", dataSource, null, "hr");

    rootSchema.add("ex", schema);
//    System.out.println(calciteConnection.getMetaData());
    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
    ResultSet rs=databaseMetaData.getTables(null,null,"%",new String[]{"TABLE"});
    ResultSet rs1=databaseMetaData.getColumns(null,null,"depts","%");

//    ResultSet rs = databaseMetaData.getTables(null, null, "%", new String[]{"TABLE"});
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select * from ex.depts");

    output(rs,System.out);
    output(rs1,System.out);
    output(resultSet, System.out);
    resultSet.close();
    statement.close();
    connection.close();
  }

  private static void output(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i=0;i<columnCount;i++){
      out.print(metaData.getColumnLabel(i+1)+" ");
    }
    out.println();
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


