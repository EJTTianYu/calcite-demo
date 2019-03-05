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

public class CalciteMysqlConnectionIns {

  public static void main(String[] args) throws Exception {

    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    info.setProperty("remarks","true");
    info.setProperty("parserFactory","org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection.getProperties());
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Class.forName("com.mysql.jdbc.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://192.168.130.7/hr");
    dataSource.setUsername("root");
    dataSource.setPassword("123456");
    Schema schema = JdbcSchema.create(rootSchema, "hr", dataSource, null, "hr");

    rootSchema.add("hr", schema);
//    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
//    ResultSet rs=databaseMetaData.getTables(null,null,"%",new String[]{"TABLE"});
//    ResultSet rs1 = databaseMetaData.getColumns(null, null, "dwf_test", "%");

    Statement statement = calciteConnection.createStatement();
    statement.execute("insert into hr.depts_insert_data (deptno) values(40)");
//    PGConnection.outputResult(resultSet,System.out,new int[]{1,2,3});
//    statement.execute(
//        "drop table hr.emps");
//    statement.executeUpdate("insert into ex.emp values ('test',1)");

//    PGConnection.outputResult(rs,System.out,new String[]{"TABLE_NAME","REMARKS"});
//    PGConnection.outputResult(rs1, System.out, new String[]{"TABLE_NAME","COLUMN_NAME","TYPE_NAME"});
//    statement.close();
    statement.close();
    connection.close();
  }
}


