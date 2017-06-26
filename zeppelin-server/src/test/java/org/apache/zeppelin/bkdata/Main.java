package org.apache.zeppelin.bkdata;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.zeppelin.util.HTTPUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by norbertchen on 2017/6/26.
 */
public class Main {
  public static void main(String[] args) throws Exception{
    Gson gson = new Gson();
    String sql = "select * from table1 left join table2 on t1.id = t2.id";
    System.out.println(new String(Base64.encodeBase64(sql.getBytes())));
    List<String> rtn = new ArrayList<>();
    String jdbcRealmUrl = "http://api.leaf.ied.com";
    String jdbcRealmPath = "/offline/sql/find_table_name?sql=%s";
    GetMethod getZeppelinUser = HTTPUtils.httpGet(jdbcRealmUrl,
        String.format(jdbcRealmPath, new String(Base64.encodeBase64(sql.getBytes()))));
    Map<String, Object> resp = gson.fromJson(getZeppelinUser.getResponseBodyAsString(),
        new TypeToken<Map<String, Object>>() {
        }.getType());
    List<String> tables = (List<String>)resp.get("data");
    System.out.println(tables);
  }
}
