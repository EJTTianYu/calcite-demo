package org.tianyu.learnJDBC;

import dwf3s.PGConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MySQLLearn {

  public static void main(String[] args) throws Exception {
    //加载驱动，使用反射知识
    Class.forName("com.mysql.jdbc.Driver");
    try {
//      建立数据库连接
      Connection connection = DriverManager
          .getConnection("jdbc:mysql://192.168.130.7:3306/hr", "root", "123456");
//      创建Statement对象
      Statement statement = connection.createStatement();
//      获取结果集
//      ResultSet resultSet = statement.executeQuery("select * from emps");
//      while (resultSet.next()) {
//        final int columnNum = resultSet.getMetaData().getColumnCount();
//        for (int i = 1; i <= columnNum; i++) {
//          System.out.print(resultSet.getString(i) + " ");
//        }
//        System.out.println();
//      }
      DatabaseMetaData dbmd=connection.getMetaData();
      ResultSet resultSet=dbmd.getColumns(null,null,"dwf_%","%");
      PGConnection.outputResult(resultSet,System.out,new String[]{"COLUMN_NAME","TYPE_NAME"});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
