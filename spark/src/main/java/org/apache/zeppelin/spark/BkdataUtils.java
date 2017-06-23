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

package org.apache.zeppelin.spark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zeppelin.utils.HTTPUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility and helper functions for the BKDATA
 */
class BkdataUtils {
  public static Logger logger = LoggerFactory.getLogger(BkdataUtils.class);
  private static Pattern r = Pattern.compile("sqlc\\.read\\.(parquet|json)\\(.*$");
  private static Pattern rn = Pattern.compile("\\(.*\\)");
  private static Gson gson = new Gson();

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
    String prefix = fetchHdfsPrefix(rt_id) + biz_id + "/" + StringUtils.join(splits, "_", 1,
        splits.length) + "_" + biz_id;
    long startTimeMills = dateFormat.parse(start_time).getTime();
    long endTimeMills = dateFormat.parse(end_time).getTime();
    List<String> paths = new LinkedList<>();
    for (long timeMills = startTimeMills; timeMills <= endTimeMills; timeMills += 3600000) {
      paths.add(prefix + pathDateFormat.format(timeMills));
    }
    return StringUtils.join(paths, ",");
  }

  public static String coreWordReplace(String line) throws Exception {
    Matcher m = r.matcher(line);
    if (m.find()) {
      boolean isError = true;
      Matcher mn = rn.matcher(m.group());
      if (mn.find()) {
        String s = mn.group();
        String toSpilit = s.replace("\"", "").replace("(", "").replace(")", "");
        String[] readArgs = toSpilit.split(",");
        logger.info("~~ sparksqlInterpreter {}", Arrays.toString(readArgs));
        //default 3 args : rt_id, start_time, end_time
        if (readArgs.length == 3) {
          String rt_id = readArgs[0];
          String start_time = readArgs[1];
          String end_time = readArgs[2];
          try {
            String newReadArg = sparkReadArgs2HdfsPath(rt_id, start_time, end_time);
            String left = line.substring(0, line.indexOf("("));
            String right = line.substring(line.indexOf(")"));
            String newCmd = left + "(\"" + newReadArg + "\"" + right;
            logger.info("~~ new cmd {}", newCmd);
            line = newCmd;
            isError = false;
          } catch (ParseException pe) {
            logger.error("~~ prase read args error - {}", pe.getMessage());
          }
        }
      }
      if (isError)
        throw new Exception("spark grammar wrong");
    }
    return line;
  }
}
