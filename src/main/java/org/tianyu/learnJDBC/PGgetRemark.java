package org.tianyu.learnJDBC;

import dwf3s.PGConnection;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class PGgetRemark {

  public static void main(String[] args) throws Exception{
    Class.forName("com.mysql.jdbc.Driver");
    String url="jdbc:mysql://192.168.130.7:3306/hr";
    Properties properties=new Properties();
    properties.setProperty("user","root");
    properties.setProperty("password","123456");
    properties.setProperty("remarks","true");
    properties.setProperty("useInformationSchema","true");
    Connection connection= DriverManager.getConnection(url,properties);
    DatabaseMetaData dmbd=connection.getMetaData();
//    ResultSet rs=dmbd.getTables(null,null,"%",new String[]{"TABLE"});
//    output(rs,System.out);
    ResultSet rs1=dmbd.getTables(null,null,"%",null);
    PGConnection.outputResult(rs1,System.out,new String[]{"REMARKS"});

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
