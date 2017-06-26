/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility and helper functions for the BKDATA
 */
public class BkdataUtils {
  public static Logger logger = LoggerFactory.getLogger(BkdataUtils.class);
  private static Pattern r = Pattern.compile("sqlc\\.read\\.(parquet|json)\\(.*$");
  private static Pattern rn = Pattern.compile("\\(.*\\)");
  private static Gson gson = new Gson();
  private static String[] notAllowdSQLPrefix = new String[]{
      "CREATE",
      "ALTER",
      "DROP",
      "DELETE",
      "UPDATE",
      "INSERT",
      "RENAME"
  };

  /**
   * 内部返回封装
   */
  public static class DataApiRtn {
    private boolean result = false;
    private String message = "";


    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public boolean isResult() {
      return result;

    }

    public void setResult(boolean result) {
      this.result = result;
    }
  }

  /**
   * BK权限类
   */
  public static class BKAuth {
    private String userName;
    private String password;
    private String bk_ticket;

    public String getUserName() {
      return userName;
    }

    public void setUserName(String userName) {
      this.userName = userName;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getBk_ticket() {
      return bk_ticket;
    }

    public void setBk_ticket(String bk_ticket) {
      this.bk_ticket = bk_ticket;
    }
  }

  public static void checkSql(String sql) throws IllegalArgumentException {
    for (String keyWord : notAllowdSQLPrefix) {
      if (sql.toUpperCase().startsWith(keyWord))
        throw new IllegalArgumentException(keyWord + " not permit");
    }
    //对SHOW做单独处理
    if (sql.toUpperCase().startsWith("SHOW")) {
      String[] sqlSplit = sql.split("\\s+");
      if (sqlSplit.length < 2)
        throw new IllegalArgumentException("SHOW syntax error");
      String keyword2nd = sqlSplit[1].toUpperCase();
      if ("CREATE".equals(keyword2nd))
        throw new IllegalArgumentException("SHOW CREATE not permit");
      else if ("COLUMNs".equals(keyword2nd))
        throw new IllegalArgumentException("SHOW COLUMNS not permit");
      if ("FULL".equals(keyword2nd) && sqlSplit.length > 2) {
        String keyword3rd = sqlSplit[2].toUpperCase();
        if ("COLUMNs".equals(keyword3rd))
          throw new IllegalArgumentException("SHOW FULL COLUMNS not permit");
      }
    }
  }

  public static BKAuth convertBKTicket2Auth(String bk_ticket) throws IOException {
    BKAuth bkAuth = new BKAuth();
    String jdbcRealmUrl = "http://api.leaf.ied.com";
    String jdbcRealmPath = "/offline/analysis/authentication?bk_ticket=%s";
    GetMethod getZeppelinUser = HTTPUtils.httpGet(jdbcRealmUrl,
        String.format(jdbcRealmPath, bk_ticket));
    Map<String, Object> resp = gson.fromJson(getZeppelinUser.getResponseBodyAsString(),
        new TypeToken<Map<String, Object>>() {
        }.getType());
    boolean result = (Boolean) resp.get("result");
    if (!result) {
      String msg = (String) resp.get("message");
      logger.error("Call offline api Failed - {}", msg);
      throw new IOException("make legal user failed");
    }
    Map<String, String> dataJdbcRealm = (Map<String, String>) resp.get("data");
    bkAuth.setBk_ticket(bk_ticket);
    bkAuth.setPassword(dataJdbcRealm.get("password"));
    bkAuth.setUserName(dataJdbcRealm.get("userName"));
    return bkAuth;
  }


  /*
  inspired from https://github.com/postgres/pgadmin3/blob/794527d97e2e3b01399954f3b79c8e2585b908dd/
  pgadmin/dlg/dlgProperty.cpp#L999-L1045
  */
  public static ArrayList<String> splitSqlQueries(String sql) {
    ArrayList<String> queries = new ArrayList<>();
    StringBuilder query = new StringBuilder();
    char character;

    Boolean antiSlash = false;
    Boolean multiLineComment = false;
    Boolean singleLineComment = false;
    Boolean quoteString = false;
    Boolean doubleQuoteString = false;

    for (int item = 0; item < sql.length(); item++) {
      character = sql.charAt(item);

      if ((singleLineComment && (character == '\n' || item == sql.length() - 1))
          || (multiLineComment && character == '/' && sql.charAt(item - 1) == '*')) {
        singleLineComment = false;
        multiLineComment = false;
        if (item == sql.length() - 1 && query.length() > 0) {
          queries.add(StringUtils.trim(query.toString()));
        }
        continue;
      }

      if (singleLineComment || multiLineComment) {
        continue;
      }

      if (character == '\\') {
        antiSlash = true;
      }

      if (character == '\'') {
        if (antiSlash) {
          antiSlash = false;
        } else if (quoteString) {
          quoteString = false;
        } else if (!doubleQuoteString) {
          quoteString = true;
        }
      }

      if (character == '"') {
        if (antiSlash) {
          antiSlash = false;
        } else if (doubleQuoteString) {
          doubleQuoteString = false;
        } else if (!quoteString) {
          doubleQuoteString = true;
        }
      }

      if (!quoteString && !doubleQuoteString && !multiLineComment && !singleLineComment
          && sql.length() > item + 1) {
        if (character == '-' && sql.charAt(item + 1) == '-') {
          singleLineComment = true;
          continue;
        }

        if (character == '/' && sql.charAt(item + 1) == '*') {
          multiLineComment = true;
          continue;
        }
      }

      if (character == ';' && !antiSlash && !quoteString && !doubleQuoteString) {
        queries.add(StringUtils.trim(query.toString()));
        query = new StringBuilder();
      } else if (item == sql.length() - 1) {
        query.append(character);
        queries.add(StringUtils.trim(query.toString()));
      } else {
        query.append(character);
      }
    }

    return queries;
  }

  /**
   * 检查用户是否具有访问否个note中的某个rt的权限
   *
   * @param rt_id
   * @param note_id
   * @param operator
   * @return
   */
  public static DataApiRtn checkAccessPrivilege(String rt_id, String note_id, String operator) {
    DataApiRtn rtn = new DataApiRtn();
    String request = "{" +
        "\"app_code\":" + "\"data_analysis\"" +
        "\"app_secret\":" + "\"Ff?41^Cao^M-gGb*Nx-TQ?M!Ej~jo8kZ*GU@&IZcyVH?Ttu3SP\"" +
        "\"operator\":" + "\"" + operator + "\"" +
        "\"note_id\":" + "\"" + note_id + "\"" +
        "\"result_table_id\":" + "\"" + rt_id + "\"" +
        "}";
    try {
      PostMethod post = HTTPUtils.httpPost("http://bk-data.apigw.o.oa.com",
          "/test/web/notebook/checkAuth/", request);
      Map<String, Object> resp = gson.fromJson(post.getResponseBodyAsString(),
          new TypeToken<Map<String, Object>>() {
          }.getType());
      post.releaseConnection();
      boolean result = (Boolean) resp.get("result");
      rtn.setResult(result);
      if (!result) {
        String msg = (String) resp.get("message");
        logger.info("{} {} {} auth failed : {}", operator, note_id, rt_id, msg);
        rtn.setMessage(msg);
      }
      return rtn;
    } catch (IOException ie) {
      rtn.setMessage(ie.getMessage());
      logger.info("{} {} {} auth failed : {}", operator, note_id, rt_id, ie.getMessage());
    }
    return rtn;
  }

  public static DataApiRtn callbackNoteRT(String rt_id, String note_id, String paragraph_id,
                                          String operator) {
    DataApiRtn rtn = new DataApiRtn();
    String request = "{" +
        "\"app_code\":" + "\"data_analysis\"" +
        "\"app_secret\":" + "\"Ff?41^Cao^M-gGb*Nx-TQ?M!Ej~jo8kZ*GU@&IZcyVH?Ttu3SP\"" +
        "\"operator\":" + "\"" + operator + "\"" +
        "\"note_id\":" + "\"" + note_id + "\"" +
        "\"paragraph_id\":" + "\"" + paragraph_id + "\"" +
        "\"result_table_id\":" + "\"" + rt_id + "\"" +
        "}";
    try {
      PostMethod post = HTTPUtils.httpPost("http://bk-data.apigw.o.oa.com",
          "/test/web/notebook/addNoteRT/", request);
      Map<String, Object> resp = gson.fromJson(post.getResponseBodyAsString(),
          new TypeToken<Map<String, Object>>() {
          }.getType());
      post.releaseConnection();
      boolean result = (Boolean) resp.get("result");
      rtn.setResult(result);
      if (!result) {
        String msg = (String) resp.get("message");
        logger.info("{} {} {} auth failed : {}", operator, note_id, rt_id, msg);
        rtn.setMessage(msg);
      }
      return rtn;
    } catch (IOException ie) {
      rtn.setMessage(ie.getMessage());
      logger.info("{} {} {} auth failed : {}", operator, note_id, rt_id, ie.getMessage());
    }
    return rtn;
  }

  public static String fetchHdfsPrefix(String rt_id) {
    String rtn = "/kafka/data/";
    try {
      GetMethod getRtType = HTTPUtils.httpGet("http://api.leaf.ied.com",
          String.format("/offline/rt/get_rt_type?rt_id=%s", rt_id));
      Map<String, Object> resp = gson.fromJson(getRtType.getResponseBodyAsString(),
          new TypeToken<Map<String, Object>>() {
          }.getType());
      boolean result = (Boolean) resp.get("result");
      if (!result) {
        String msg = (String) resp.get("message");
        logger.error("Call offline api Failed - {}", msg);
        throw new IOException("Result table not found");
      }
      Map<String, String> data = (Map<String, String>) resp.get("data");
      if ("batch".equals(data.get("type")))
        rtn = "/api/flow/";
    } catch (IOException ie) {
      logger.error("~~ error in get rt type :", ie);
    }
    return rtn;
  }

  public static String sparkReadArgs2HdfsPath(String rt_id, String start_time, String end_time)
      throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH") {
      {
        setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
      }
    };
    SimpleDateFormat pathDateFormat = new SimpleDateFormat("/yyyy/MM/dd/HH") {
      {
        setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
      }
    };
    String[] splits = rt_id.split("_");
    String biz_id = splits[0];
    // /kafa/data/biz_id/table_name_biz_id
    String prefix = fetchHdfsPrefix(rt_id) + biz_id + "/" + resultTableConvert(rt_id);
    long startTimeMills = dateFormat.parse(start_time).getTime();
    long endTimeMills = dateFormat.parse(end_time).getTime();
    List<String> paths = new LinkedList<>();
    for (long timeMills = startTimeMills; timeMills <= endTimeMills; timeMills += 3600000) {
      paths.add(prefix + pathDateFormat.format(timeMills));
    }
    return StringUtils.join(paths, ",");
  }

  /**
   * 1、敏感路径替换
   * 2、权限控制
   *
   * @param line
   * @param note_id
   * @param userName
   * @return
   * @throws Exception
   */
  public static DataApiRtn sparkCoreWordReplace(String line, String note_id, String paragraph_id,
                                                String userName)
      throws IllegalAccessException, ParseException, IllegalArgumentException {
    DataApiRtn rtn = new DataApiRtn();
    rtn.setMessage(line);
    Matcher m = r.matcher(line);
    if (m.find()) {
      boolean isError = true;
      Matcher mn = rn.matcher(m.group());
      if (mn.find()) {
        String s = mn.group();
        String toSpilit = s.replace("\"", "").replace("(", "").replace(")", "");
        String[] readArgs = toSpilit.split(",");
        logger.info("~~ Spark interpreter {}", Arrays.toString(readArgs));
        if (readArgs.length == 3) {
          String rt_id = readArgs[0];
          DataApiRtn cap = checkAccessPrivilege(rt_id, note_id, userName);
          if (!cap.isResult()) {
            String errMsg = userName + " access " + rt_id + " in note(" + note_id + ") failed";
            throw new IllegalAccessException(errMsg);
          }
          String start_time = readArgs[1];
          String end_time = readArgs[2];
          String newReadArg = sparkReadArgs2HdfsPath(rt_id, start_time, end_time);
          String left = line.substring(0, line.indexOf("("));
          String right = line.substring(line.indexOf(")"));
          String newCmd = left + "(\"" + newReadArg + "\"" + right;
          logger.info("~~ new cmd - {}", newCmd);
          rtn.setMessage(newCmd);
          rtn.setResult(true);
          callbackNoteRT(rt_id, note_id, paragraph_id, userName);
          isError = false;
        }
      }
      if (isError)
        throw new IllegalArgumentException("spark argument wrong");
    }
    return rtn;
  }

  public static boolean isInteger(String str) {
    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
    return pattern.matcher(str).matches();
  }

  public static List<String> parseSQLTablename(String sql) throws IOException {
    String url = "http://api.leaf.ied.com";
    String path = "/offline/sql/find_table_name?sql=%s";
    GetMethod getZeppelinUser = HTTPUtils.httpGet(url,
        String.format(path, new String(Base64.encodeBase64(sql.getBytes()))));
    Map<String, Object> resp = gson.fromJson(getZeppelinUser.getResponseBodyAsString(),
        new TypeToken<Map<String, Object>>() {
        }.getType());
    boolean result = (Boolean) resp.get("result");
    if (!result) {
      String msg = (String) resp.get("message");
      logger.error("Call offline api Failed - {}", msg);
      throw new IOException("Parse SQL error");
    }
    return (List<String>) resp.get("data");
  }

  /**
   * biz_id_table_name to table_name_biz_id
   *
   * @param rt_id
   * @return
   */
  public static String resultTableConvert(String rt_id) {
    String[] rtSplit = rt_id.split("_");
    if (rtSplit.length < 2)
      throw new IllegalArgumentException("Result table syntax error");
    return StringUtils.join(rtSplit, "_", 1, rtSplit.length) + "_" + rtSplit[0];
  }

  public static DataApiRtn jdbcCoreWorkReplace(String sql, String note_id, String paragrapth_id,
                                               String userName)
      throws IOException,IllegalAccessException {
    DataApiRtn rtn = new DataApiRtn();
    rtn.setMessage(sql);
    rtn.setResult(true);
    String uppderSQL = sql.toUpperCase();
    if (uppderSQL.startsWith("SELECT")) {
      for (String tableName : parseSQLTablename(sql)) {
        DataApiRtn cap = checkAccessPrivilege(tableName, note_id, userName);
        if (!cap.isResult()) {
          String errMsg = userName + " access " + tableName + " in note(" + note_id + ") failed";
          throw new IllegalAccessException(errMsg);
        }
        sql = sql.replace(tableName, resultTableConvert(tableName));
      }
      rtn.setMessage(sql);
    } else if (uppderSQL.startsWith("USE")) {
      String[] useSplit = sql.split("\\s+");
      if (useSplit.length != 2)
        throw new IllegalArgumentException("Use syntax error");
      String biz_id = useSplit[1];
      if (!isInteger(biz_id))
        throw new IllegalArgumentException(String.format("Biz_id %s error", biz_id));
      useSplit[1] = "mapleleaf_" + biz_id;
      rtn.setMessage(StringUtils.join(useSplit, " "));
    } else if (uppderSQL.startsWith("DESC") || uppderSQL.startsWith("DESCRIBE")) {
      String[] descSplit = sql.split("\\s+");
      if (descSplit.length != 2)
        throw new IllegalArgumentException("DESCRIBE syntax error");
      String rt_id = descSplit[1];
      descSplit[1] = resultTableConvert(rt_id);
      DataApiRtn cap = checkAccessPrivilege(rt_id, note_id, userName);
      if (!cap.isResult()) {
        String errMsg = userName + " access " + rt_id + " in note(" + note_id + ") failed";
        throw new IllegalAccessException(errMsg);
      }
      callbackNoteRT(rt_id, note_id, paragrapth_id, userName);
      rtn.setMessage(StringUtils.join(descSplit, " "));
    } else if (uppderSQL.startsWith("SHOW")) {
      //pass show tables
    } else
      rtn.setResult(false);
    return rtn;
  }


}
