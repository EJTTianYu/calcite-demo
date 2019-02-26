package org.tianyu.learnJDBC.ini;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class MySQLUsingIni {
  private String driver;
  private String url;
  private String user;
  private String pass;

  public void init(String paramFile) throws Exception{
    Properties properties=new Properties();
    properties.load(new FileInputStream(paramFile));
    driver=properties.getProperty("driver");
    url=properties.getProperty("url");
    user=properties.getProperty("user");
    pass=properties.getProperty("pass");
  }
  public void createTable(String sql) throws Exception{
    Class.forName(driver);
    try{
      Connection connection= DriverManager.getConnection(url,user,pass);
      Statement statement=connection.createStatement();
      statement.executeUpdate(sql);
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }
  public static void main(String[] args) throws Exception{
    MySQLUsingIni mySQLUsingIni=new MySQLUsingIni();
    mySQLUsingIni.init("/Users/tianyu/git_project/dwf3s/src/main/java/org/tianyu/learnJDBC/ini/mysql.ini");
    mySQLUsingIni.createTable("create database jdbc_Test ");
    System.out.println("success");
  }
}
