package org.apache.zeppelin.bkdata;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.zeppelin.util.BkdataUtils;
import org.apache.zeppelin.util.HTTPUtils;

import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by norbertchen on 2017/6/26.
 */
public class Main {
  public static void main(String[] args) throws Exception{
//    Gson gson = new Gson();
//    String sql = "(select type as user_type,\n" +
//        "       1 as pkg_type,\n" +
//        "       sum(if(pkg_1==1,1,0)) as pkg_num,\n" +
//        "\t   count(*) as total_num,\n" +
//        "       round((sum(if(pkg_1==1,1,0))/count(*)),2) as pkg_rate\n" +
//        "from ubm_cv_dl_tgp_newpkg_3\n" +
//        "group by type)\n" +
//        "union\n" +
//        "(select type as user_type,\n" +
//        "       2 as pkg_type,\n" +
//        "       sum(if(pkg_2==1,1,0)) as pkg_num,\n" +
//        "\t   count(*) as total_num,\n" +
//        "       round((sum(if(pkg_2==1,1,0))/count(*)),2) as pkg_rate\n" +
//        "from ubm_cv_dl_tgp_newpkg_3\n" +
//        "group by type)\n" +
//        "union\n" +
//        "(select type as user_type,\n" +
//        "       3 as pkg_type,\n" +
//        "       sum(if(pkg_3==1,1,0)) as pkg_num,\n" +
//        "\t   count(*) as total_num,\n" +
//        "       round((sum(if(pkg_3==1,1,0))/count(*)),2) as pkg_rate\n" +
//        "from ubm_cv_dl_tgp_newpkg_3\n" +
//        "group by type)\n";
//    System.out.println(new String(Base64.encodeBase64(sql.getBytes())));
//    List<String> rtn = new ArrayList<>();
//    String jdbcRealmUrl = "http://api.leaf.ied.com";
//    String jdbcRealmPath = "/offline/sql/find_table_name?sql=%s";
//    GetMethod getZeppelinUser = HTTPUtils.httpGet(jdbcRealmUrl,
//        String.format(jdbcRealmPath, new String(Base64.encodeBase64(sql.getBytes()))));
//    Map<String, Object> resp = gson.fromJson(getZeppelinUser.getResponseBodyAsString(),
//        new TypeToken<Map<String, Object>>() {
//        }.getType());
//    List<String> tables = (List<String>)resp.get("data");
//    System.out.println(tables);
//    String test = "a1f2f105_abc_123";
//    Pattern r = Pattern.compile("^\\s*[a-z]+[a-z0-9A-Z_]+_\\d+\\s*$");
//    Matcher m = r.matcher(test);
//    if (m.find()){
//      System.out.println(m.group());
//    }
//    String tmp = "\nshow tables; ";"
//    System.out.println("1"+tmp.trim()+"1");
    String line = "%spark.pyspark textFile = sc.textFile(\"/kafka/data/\")";
    Pattern r = Pattern.compile("sc\\.textFile\\(.*$");
    Pattern rn = Pattern.compile("\\(.*\\)");
    Matcher m = r.matcher(line);
    if (m.find()) {
      System.out.println(m.group());
      Matcher mn = rn.matcher(m.group());
      if (mn.find()) {
        System.out.println(mn.group());
      }
    }

  }

  private static void test( List<String> abc){
    abc.add("ccc");
  }
}
