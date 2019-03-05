import cn.edu.thu.dwf3s.DWF3sFactory;
import cn.edu.thu.dwf3s.dataset.bean.DataSet;
import cn.edu.thu.dwf3s.dataset.iface.IDataSetReader;
import cn.edu.thu.dwf3s.datasource.bean.DataSource;
import cn.edu.thu.dwf3s.datasource.bean.DataSourceType;
import cn.edu.thu.dwf3s.utils.Utils;
import cn.edu.thu.dwf3s.postgresql.reader.PostgresqIDataSetReader;
import java.util.List;
import java.util.Map;

public class PGReadData {

  public static void main(String[] args) throws ClassNotFoundException{
    DataSource dataSource = new DataSource("testPostgreSQLDataSource", "3s SDK单元测试",
        "192.168.130.7", 5432,
        "postgres", "postgres", "postgres.pgtest", DataSourceType.POSTGRESQL, null, null, null, null);
    DataSet dwfDataSet = new DataSet(dataSource, "testPostgreSqlDataSet", "3s SDK单元测试",
        "select ? from dwf3s", null, "select * from dwf3s", null);
    IDataSetReader dataSetReader=DWF3sFactory.getDataSetReader(dataSource);
    String[] parameters = {"*"};
    List<Map<String, Object>> dataSet = dataSetReader
        .readDataSet(dwfDataSet, parameters, 1, 2);
    Utils.show(dataSet);
  }

}
