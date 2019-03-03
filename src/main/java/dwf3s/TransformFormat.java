package dwf3s;


public class TransformFormat {

  public static ColumnType typeConvert(String pgType) {
    switch (pgType) {
      case "bit"://一个二进制位
        return ColumnType.INT;
      case "bool":
        return ColumnType.BOOLEAN;
      case "bytea"://二进制数组
        return ColumnType.BYTES;
      case "char":
        return ColumnType.STRING;
      case "cidr"://IPv4 或者 IPv6 网络地址
        return ColumnType.STRING;
      case "date":
        return ColumnType.DATE;
      case "decimal":
        return ColumnType.FLOAT;
      case "float4":
        return ColumnType.FLOAT;
      case "float8":
        return ColumnType.FLOAT;
      case "inet"://IPv4 或者 IPv6 网络地址
        return ColumnType.STRING;
      case "int2":
        return ColumnType.INT;
      case "int4":
        return ColumnType.INT;
      case "int8":
        return ColumnType.LONG;
      case "interval":
        return ColumnType.STRING;
      case "json":
        return ColumnType.STRING;
      case "jsonb":
        return ColumnType.STRING;
      case "macaddr"://mac地址
        return ColumnType.STRING;
      case "money"://ex：￥1.00
        return ColumnType.STRING;
      case "serial2":
        return ColumnType.LONG;
      case "serial4":
        return ColumnType.LONG;
      case "serial8":
        return ColumnType.LONG;
      case "text":
        return ColumnType.STRING;
      case "time"://ex：21:00:00
        return ColumnType.STRING;
      case "timestamp":
        return ColumnType.TIMESTAMP;
      case "timestampz":
        return ColumnType.TIMESTAMP;
      case "timez":
        return ColumnType.STRING;
      case "uuid":
        return ColumnType.UUID;
      case "varbit":
        return ColumnType.STRING;
      case "varchar":
        return ColumnType.STRING;
      case "xml":
        return ColumnType.STRING;
      default:
        return ColumnType.STRING;
    }
  }

  public static String typeConvert(ColumnType columnType) {
    switch (columnType) {
      case INT:
        return "int8";
      case FLOAT:
        return "float8";
      case LONG:
        return "int8";
      case DATE:
        return "date";
      case TIMESTAMP:
        return "timestamp";
      case BOOLEAN:
        return "bool";
      case BYTES:
        return "bytea";
      case UUID:
        return "uuid";
      case STRING:
        return "varchar";
      default:
        return "varchar";
    }
  }

  public static void typeChangeTest(String pgType) {
    switch (pgType) {
      case "test":
        System.out.println("success");
    }
  }

  public static void main(String[] args) {
//    TransformFormat.typeChangeTest("test");

//    for (ColumnType s:ColumnType.values()) {
//      System.out.println(s);
//    }
    TransformFormat.typeConvert("fuck");
  }

}
