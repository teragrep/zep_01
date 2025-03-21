/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teragrep.zep_01.interpreter;

import com.teragrep.zep_01.scheduler.Scheduler;
import com.teragrep.zep_01.scheduler.SchedulerFactory;

import java.util.Properties;

/**
 * Interpreter that only accept long value and sleep for such period
 */
public class SleepInterpreter extends Interpreter {

  public SleepInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    try {
      Thread.sleep(Long.parseLong(st));
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    } catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public Scheduler getScheduler() {
    if (Boolean.parseBoolean(getProperty("zeppelin.SleepInterpreter.parallel", "false"))) {
      return SchedulerFactory.singleton().createOrGetParallelScheduler(
          "Parallel-" + SleepInterpreter.class.getName(), 10);
    }
    return super.getScheduler();
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }
}
