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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

/**
 * Utility and helper functions for the BKDATA
 */
class BkdataUtils {
  public static Logger logger = LoggerFactory.getLogger(BkdataUtils.class);

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

  public static String sparkReadArgs2HdfsPath(String rt_id,String start_time,String end_time)
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
    String prefix = "/kafka/data/" + splits[0] + "/" + StringUtils.join(splits, "_", 1,
        splits.length);
    long startTimeMills = dateFormat.parse(start_time).getTime();
    long endTimeMills = dateFormat.parse(end_time).getTime();
    List<String> paths = new LinkedList<>();
    for(long timeMills = startTimeMills;timeMills <= endTimeMills; timeMills += 3600000){
      paths.add(prefix + pathDateFormat.format(timeMills));
    }
    return StringUtils.join(paths,",");
  }
}
