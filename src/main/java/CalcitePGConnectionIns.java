import dwf3s.PGConnection;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

public class CalcitePGConnectionIns {

  public static void main(String[] args) throws Exception {

    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
//    info.setProperty("remarks","true");
//    info.setProperty("useInformationSchema","true");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection.getProperties());
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Class.forName("org.postgresql.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:postgresql://192.168.130.7:5432/postgres");
    dataSource.setUsername("postgres");
    dataSource.setPassword("postgres");
    Schema schema = JdbcSchema.create(rootSchema, "ex", dataSource, null, "pgtest");

    rootSchema.add("ex", schema);
    DatabaseMetaData databaseMetaData = calciteConnection.getMetaData();
    ResultSet rs=databaseMetaData.getTables(null,null,"%",new String[]{"TABLE"});
    ResultSet rs1 = databaseMetaData.getColumns(null, null, "dwf_%", "%");

//    Statement statement = calciteConnection.createStatement();
//    ResultSet resultSet = statement.executeQuery(
//        "select * from ex.depts limit 2 offset 1");

    PGConnection.outputResult(rs,System.out,new String[]{"TABLE_NAME","REMARKS"});
    PGConnection.outputResult(rs1, System.out, new String[]{"TABLE_NAME","COLUMN_NAME","TYPE_NAME"});
//    statement.close();
    connection.close();
  }
}


