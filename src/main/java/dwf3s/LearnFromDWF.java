package dwf3s;

public class LearnFromDWF {

  public void learnRegex(){
    String path="level1.level2";
    for (String s:path.split("\\.")
    ) {
      System.out.println(s);
    }
  }
  public void learnIndexOf(){
    String path="level1.level2";
    System.out.println(path.indexOf("level2"));
  }
  public String switchTest(String testString){
    switch (testString){
      case "test":return "ok";
    }
    return "false";
  }
  public static void main(String[] args) {
//    new LearnFromDWF().learnIndexOf();
    System.out.println(new LearnFromDWF().switchTest("fuck"));
  }
}
