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
//    String sql = "select * from table1 left join table2 on t1.id = t2.id";
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
//    String tmp = "\nshow tables; ";
//    System.out.println("1"+tmp.trim()+"1");
    List<String> list = new LinkedList<String>(){
      {
        add("aaa");
      }
    };
    test(list);
    System.out.println(Arrays.toString(list.toArray()));
  }

  private static void test( List<String> abc){
    abc.add("ccc");
  }
}
