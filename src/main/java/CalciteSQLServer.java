import dwf3s.PGConnection;
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

public class CalciteSQLServer {

  public static void main(String[] args) throws ClassNotFoundException, SQLException ,Exception{
    long startTime=System.currentTimeMillis();
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // 本地Schema.
    // Schema schema = ReflectiveSchema.create(calciteConnection, rootSchema, "hr",
    // new HrSchema());

    /**
     * 此处为不同数据源的实际操作部分，BasicDataSource为Apache DBCP的连接池
     */
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:sqlserver://192.168.130.30:1433;databaseName=iotdb");
    dataSource.setUsername("gouwang");
    dataSource.setPassword("gouwang");
    dataSource.addConnectionProperty("remarks", "true");
    dataSource.addConnectionProperty("useInformationSchema", "true");
    /**
     * 此处创建Schema的时候，param1指定父模式，param5的schema为数据库中的模式，需要匹配，param2暴露出来的name用作下面的rootSchema的add方法
     */
    Schema schema = JdbcSchema.create(rootSchema, "dbo", dataSource, null, "dbo");

    rootSchema.add("dbo", schema);
    /**
     * 获取数据库的元信息，实际之后的操作步骤同JDBC的操作，值得注意的一点是sql语句中的PG为刚才暴露出的name
     */
    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
    //用于获取所有的表
//    ResultSet rs = databaseMetaData.getTables(null, null, "%", new String[]{"TABLE"});
    //用于获取特定表的所有列
//    ResultSet rs1 = databaseMetaData.getColumns(null, null, "dwf%", "%");

    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet=statement.executeQuery(
        "select * from dbo.dwf_sql_test");

//    PGConnection.outputResult(rs, System.out,new String[]{"TABLE_NAME","REMARKS"});
//    PGConnection.outputResult(rs1,System.out,new String[]{"TABLE_NAME","COLUMN_NAME","REMARKS"});
    output(resultSet, System.out);
    System.out.println(System.currentTimeMillis()-startTime);
//    rs.close();
//    rs1.close();
//    resultSet.close();
    statement.close();
    connection.close();
  }

  private static void output(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      out.print(metaData.getColumnLabel(i + 1) + " ");
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
    out.println("--------------------------");
  }
}



