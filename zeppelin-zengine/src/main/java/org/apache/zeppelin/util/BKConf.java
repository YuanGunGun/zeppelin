package org.apache.zeppelin.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by norbertchen on 2017/7/11.
 */
public class BKConf {
  public static String APP_CODE = "data_analysis";
  public static String APP_SECRET = "Ff?41^Cao^M-gGb*Nx-TQ?M!Ej~jo8kZ*GU@&IZcyVH?Ttu3SP";
  public static String[] notAllowdSQLPrefix = new String[]{
      "CREATE",
      "ALTER",
      "DROP",
      "DELETE",
      "UPDATE",
      "INSERT",
      "RENAME",
      "KILL"
  };
  public static List<String> allowedRepl = new ArrayList<String>() {
    {
      add("tspider");
      add("spark");
      add("sql");
      add("spark.sql");
      add("md");
      add("web_db");
      add("offline_db");
      add("spark.pyspark");
      add("pyspark");
    }
  };
}
