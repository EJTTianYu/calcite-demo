package dwf3s;

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

public class PGConnection {

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
  /**
   * 用于指定结果集的输出
   *
   * @param resultSet 结果集
   * @param out 输出流
   * @param indexs 用于指定要输出的int列
   */

  public void outputResult(ResultSet resultSet, PrintStream out, int[] indexs) throws Exception {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i : indexs) {
      out.print(metaData.getColumnLabel(i) + " ");
    }
    out.println();
    while (resultSet.next()) {
      for (int i : indexs) {
        out.print(resultSet.getString(i)+" ");
      }
      out.println();
    }
    out.println("--------------------------");
  }
  /**
   * 用于指定结果集的输出
   *
   * @param resultSet 结果集
   * @param out 输出流
   * @param indexs 用于指定要输出的String列
   */

  public void outputResult(ResultSet resultSet, PrintStream out, String[] indexs) throws Exception {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    out.println();
    while (resultSet.next()) {
      for (String i : indexs) {
        out.print(resultSet.getString(i)+" ");
      }
      out.println();
    }
    out.println("--------------------------");
  }

  /**
   * 输出数据库实例的表名，表描述，列名，列类型
   */
  public void returnSchemaInformation() throws Exception {
    Class.forName("org.postgresql.Driver");
    String url = "jdbc:postgresql://192.168.130.7:5432/postgres";
    Properties properties = new Properties();
    properties.setProperty("user", "postgres");
    properties.setProperty("password", "postgres");
//    properties.setProperty("remarks","true");
//    properties.setProperty("useInformationSchema","true");
    Connection connection = DriverManager.getConnection(url, properties);
    DatabaseMetaData dmbd = connection.getMetaData();
    //获取所有表的表描述，其中，schemaPattern和tableNamePattern和最后一个参数均为筛选条件
    ResultSet rs = dmbd.getTables(null, null, "%", new String[]{"TABLE"});
    outputResult(rs, System.out,new int[]{2,3,5});
    //获取所有列的列描述，其中，schemaPattern和tableNamePattern和最后一个参数均为筛选条件
    ResultSet rs1 = dmbd.getColumns(null, null, "dwf%", "%");
    outputResult(rs1, System.out,new int[]{2,3,4,6,12,13,18});
    ResultSet rs2=dmbd.getPrimaryKeys(null,null,"");
    outputResult(rs2,System.out,new int[]{2,3,4});
  }

  public void queryUsingCalcite() throws Exception {
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
    Class.forName("org.postgresql.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:postgresql://192.168.130.7:5432/postgres");
    dataSource.setUsername("postgres");
    dataSource.setPassword("postgres");
    /**
     * 此处创建Schema的时候，param1指定父模式，param5的schema为数据库中的模式，需要匹配，param2暴露出来的name用作下面的rootSchema的add方法
     */
    Schema schema = JdbcSchema.create(rootSchema, "PG", dataSource, null, "PGtest");

    rootSchema.add("PG", schema);
    /**
     * 获取数据库的元信息，实际之后的操作步骤同JDBC的操作，值得注意的一点是sql语句中的PG为刚才暴露出的name
     */
    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
    //用于获取所有的表
    ResultSet rs = databaseMetaData.getTables(null, null, "%", new String[]{"TABLE"});
    //用于获取特定表的所有列
    ResultSet rs1 = databaseMetaData.getColumns(null, null, "dwf3s", "%");

    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        //此语句的PG为刚才rootSchema中的name
        "select * from PG.company");

//    outputResult(rs, System.out);
//    outputResult(rs1, System.out);
//    outputResult(resultSet, System.out);
    rs.close();
    rs1.close();
    resultSet.close();
    statement.close();
    connection.close();
  }

  public static void main(String[] args) throws Exception {
    new PGConnection().returnSchemaInformation();
  }

}
