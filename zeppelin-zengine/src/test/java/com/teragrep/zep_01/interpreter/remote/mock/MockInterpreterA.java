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

package com.teragrep.zep_01.interpreter.remote.mock;

import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResult.Code;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import com.teragrep.zep_01.scheduler.Scheduler;
import com.teragrep.zep_01.scheduler.SchedulerFactory;

import java.util.List;
import java.util.Properties;

public class MockInterpreterA extends Interpreter {

  private String lastSt;

  public MockInterpreterA(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    //new RuntimeException().printStackTrace();
  }

  @Override
  public void close() {
  }

  public String getLastStatement() {
    return lastSt;
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {
    if (getProperties().containsKey("progress")) {
      context.setProgress(Integer.parseInt(getProperty("progress")));
    }
    try {
      Thread.sleep(Long.parseLong(st));
      this.lastSt = st;
    } catch (NumberFormatException | InterruptedException e) {
      throw new InterpreterException(e);
    }
    return new InterpreterResult(Code.SUCCESS, st);
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) {
    return null;
  }

  @Override
  public Scheduler getScheduler() {
    if (getProperty("parallel") != null && getProperty("parallel").equals("true")) {
      return SchedulerFactory.singleton().createOrGetParallelScheduler("interpreter_" + this.hashCode(), 10);
    } else {
      return SchedulerFactory.singleton().createOrGetFIFOScheduler("interpreter_" + this.hashCode());
    }
  }
}
