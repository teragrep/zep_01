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

package com.teragrep.zep_01.spark;


import org.apache.commons.lang3.exception.ExceptionUtils;
import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterGroup;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.LazyOpenInterpreter;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterEventClient;
import com.teragrep.zep_01.python.PythonInterpreterTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;


@Disabled(value="Won't build because it depends on Spark212 being available, setup fails")
class PySparkInterpreterTest extends PythonInterpreterTest {

  private RemoteInterpreterEventClient mockRemoteEventClient = mock(RemoteInterpreterEventClient.class);

  @Override
  @BeforeEach
  public void setUp() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "Zeppelin Test");
    properties.setProperty("zeppelin.spark.useHiveContext", "false");
    properties.setProperty("zeppelin.spark.maxResult", "3");
    properties.setProperty("zeppelin.spark.importImplicit", "true");
    properties.setProperty("zeppelin.pyspark.python", "python");

    properties.setProperty("zeppelin.python.gatewayserver_address", "127.0.0.1");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    // create interpreter group
    intpGroup = new InterpreterGroup();
    intpGroup.put("note", new LinkedList<Interpreter>());

    InterpreterContext context = InterpreterContext.builder()
        .setInterpreterOut(new InterpreterOutput())
        .setIntpEventClient(mockRemoteEventClient)
        .build();
    InterpreterContext.set(context);
    LazyOpenInterpreter sparkInterpreter =
        new LazyOpenInterpreter(new SparkInterpreter(properties));

    intpGroup.get("note").add(sparkInterpreter);
    sparkInterpreter.setInterpreterGroup(intpGroup);

    interpreter = new LazyOpenInterpreter(new PySparkInterpreter(properties));
    intpGroup.get("note").add(interpreter);
    interpreter.setInterpreterGroup(intpGroup);

    interpreter.open();
  }

  @Override
  @AfterEach
  public void tearDown() throws InterpreterException {
    intpGroup.close();
    intpGroup = null;
    interpreter = null;
  }


  @Override
  @Test
  public void testFailtoLaunchPythonProcess() throws InterpreterException {
    tearDown();

    intpGroup = new InterpreterGroup();

    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "Zeppelin Test");
    properties.setProperty("spark.pyspark.python", "invalid_python");
    properties.setProperty("zeppelin.python.gatewayserver_address", "127.0.0.1");
    properties.setProperty("zeppelin.spark.maxResult", "3");

    interpreter = new LazyOpenInterpreter(new PySparkInterpreter(properties));
    interpreter.setInterpreterGroup(intpGroup);
    Interpreter sparkInterpreter =
            new LazyOpenInterpreter(new SparkInterpreter(properties));
    sparkInterpreter.setInterpreterGroup(intpGroup);

    intpGroup.put("note", new LinkedList<Interpreter>());
    intpGroup.get("note").add(interpreter);
    intpGroup.get("note").add(sparkInterpreter);


    InterpreterContext.set(getInterpreterContext());

    try {
      interpreter.interpret("1+1", getInterpreterContext());
      fail("Should fail to open PySparkInterpreter");
    } catch (InterpreterException e) {
      String stacktrace = ExceptionUtils.getStackTrace(e);
      assertTrue(stacktrace.contains("No such file or directory"), stacktrace);
    }
  }

  @Override
  protected InterpreterContext getInterpreterContext() {
    InterpreterContext context = super.getInterpreterContext();
    context.setIntpEventClient(mockRemoteEventClient);
    return context;
  }
}
