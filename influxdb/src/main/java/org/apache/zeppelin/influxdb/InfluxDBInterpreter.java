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

package org.apache.zeppelin.influxdb;

import java.io.IOException;
import java.util.*;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

/**
 * InfluxDB interpreter for Zeppelin.
 */
public class InfluxDBInterpreter extends Interpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBInterpreter.class);

  private static final char WHITESPACE = ' ';
  private static final char NEWLINE = '\n';
  private static final char TAB = '\t';
  private static final String EMPTY_COLUMN_VALUE = "";
  private static final String TABLE_MAGIC_TAG = "%table ";

  public InfluxDBInterpreter(Properties property) {
    super(property);
  }


  /*
  inspired from https://github.com/postgres/pgadmin3/blob/794527d97e2e3b01399954f3b79c8e2585b908dd/
  pgadmin/dlg/dlgProperty.cpp#L999-L1045
 */
  protected ArrayList<String> splitSqlQueries(String sql) {
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


  @Override
  public void open() {
    for (Map.Entry<Object, Object> e : property.entrySet()) {
      String key = e.getKey().toString();
      String value = (e.getValue() == null) ? "" : e.getValue().toString();
      LOGGER.debug("Property: key: {}, value: {}", key, value);
    }
  }

  protected String executeQuery(InfluxDB idb, String cmd, String toConnectDB) {
    StringBuilder msg = new StringBuilder();
    try {
      QueryResult query = idb.query(new Query(cmd, toConnectDB));
      if (query.hasError()) {
        LOGGER.error("InfluxDB Query Error ", query.getError());
        msg.append(query.getError());
      } else {
        msg.append(TABLE_MAGIC_TAG);
        List<QueryResult.Result> results = query.getResults();
        LOGGER.debug("InfluxDB Query result '{}'", results.toString());
        QueryResult.Result result = results.get(0);
        if (result == null || result.getSeries() == null) {
          LOGGER.debug("InfluxDB Query result 0 '{}'", result);
          LOGGER.debug("InfluxDB Query result 0 getSeries '{}'", result.getSeries());
          return "No data from query.";
        }
        QueryResult.Series data = result.getSeries().get(0);
        Iterator<String> columns = data.getColumns().iterator();
        Iterator<List<Object>> rows = data.getValues().iterator();
        while (columns.hasNext()) {
          msg.append(sanitize(columns.next()));
          if (columns.hasNext()) {
            msg.append(TAB);
          } else {
            msg.append(NEWLINE);
          }
        }
        while (rows.hasNext()) {
          Iterator<Object> row = rows.next().iterator();
          while (row.hasNext()) {
            Object elem = row.next();
            elem = (elem != null) ? elem : "";
            msg.append(sanitize(elem.toString()));
            if (row.hasNext()) {
              msg.append(TAB);
            } else {
              msg.append(NEWLINE);
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error ", e);
      msg = new StringBuilder();
      msg.append(e);
    }
    return msg.toString();
  }


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    LOGGER.info("Run influxDB command '{}'", cmd);

    StringBuilder msg = new StringBuilder();
    InfluxDB idb = InfluxDBFactory.connect(property.getProperty("default.url"),
            property.getProperty("default.user"), property.getProperty("default.password"));
    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    ArrayList<String> multipleSqlArray = splitSqlQueries(cmd);
    String toConnectDB = property.getProperty("default.database");
    for (int i = 0; i < multipleSqlArray.size(); i++) {
      String sqlToExecute = multipleSqlArray.get(i);
//      interpreterResult.add(InterpreterResult.Type.TEXT,
//              String.format("DEBUG -> %s.", sqlToExecute));
      // 如果是use db,自动切换db,不执行实际命令。
      String[] commandSplit = sqlToExecute.trim().toLowerCase().split("\\s+");
      if (commandSplit[0].equals("use")) {
        if (commandSplit.length < 2){
          interpreterResult.add("Command `use` wrong, eg : use dbName.");
          return new InterpreterResult(Code.ERROR, interpreterResult.message());
        }
        toConnectDB = commandSplit[1];
        interpreterResult.add(InterpreterResult.Type.TEXT,
                "Query executed successfully.");
        continue;
      }
      interpreterResult.add(executeQuery(idb, sqlToExecute, toConnectDB));
    }
    return interpreterResult;
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        InfluxDBInterpreter.class.getName() + this.hashCode(), 5);
  }

//  @Override
//  public List<InterpreterCompletion> completion(String buf, int cursor) {
//    return null;
//  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public void close() {}

  /**
 * For %table response replace Tab and Newline characters from the content.
 */
  private String sanitize(String str) {
    if (str == null) {
      return EMPTY_COLUMN_VALUE;
    }
    return str.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE);
  }
}
