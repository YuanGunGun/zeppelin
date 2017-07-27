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

package org.apache.zeppelin.query;

import java.util.*;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.util.BkdataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query interpreter for Zeppelin.
 */
public class QueryInterpreter extends Interpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryInterpreter.class);


  public QueryInterpreter(Properties property) {
    super(property);
  }


  @Override
  public void open() {
    for (Map.Entry<Object, Object> e : property.entrySet()) {
      String key = e.getKey().toString();
      String value = (e.getValue() == null) ? "" : e.getValue().toString();
      LOGGER.debug("Property: key: {}, value: {}", key, value);
    }
  }

  protected String executeQuery(String cmd) {
    StringBuilder msg = new StringBuilder();
    return msg.toString();
  }


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    LOGGER.info("Run influxDB command '{}'", cmd);

    StringBuilder msg = new StringBuilder();
    property.getProperty("default.url");
    property.getProperty("default.user");
    property.getProperty("default.password");
    InterpreterResult interpreterResult = new InterpreterResult(Code.SUCCESS);
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
        QueryInterpreter.class.getName() + this.hashCode(), 5);
  }

//  @Override
//  public List<InterpreterCompletion> completion(String buf, int cursor) {
//    return null;
//  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public void close() {}
}
