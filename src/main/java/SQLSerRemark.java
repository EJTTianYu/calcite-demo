import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SQLSerRemark {

  public static void main(String[] args) throws Exception{
    long startTime=System.currentTimeMillis();
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    String url="jdbc:sqlserver://192.168.130.30:1433;databaseName=iotdb";
    Properties properties=new Properties();
    properties.setProperty("user","gouwang");
    properties.setProperty("password","gouwang");
    properties.setProperty("remarks","true");
    properties.setProperty("useInformationSchema","true");
    Connection connection= DriverManager.getConnection(url,properties);
//    DatabaseMetaData dmbd=connection.getMetaData();
//    ResultSet rs=dmbd.getTables(null,null,"%",new String[]{"TABLE"});
//    output(rs,System.out);
//    ResultSet rs1=dmbd.getTables(null,null,"dwf%",null);
//    PGConnection.outputResult(rs1,System.out,new String[]{"TABLE_NAME","REMARKS"});
    Statement statement = connection.createStatement();
    ResultSet resultSet=statement.executeQuery(
        "select * from dbo.dwf_sql_test");
    output(resultSet,System.out);
    System.out.println(System.currentTimeMillis()-startTime);

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
