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

import org.apache.zeppelin.interpreter.xref.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectRegistryImpl;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClientImpl;
import org.apache.zeppelin.interpreter.xref.Code;
import org.apache.zeppelin.interpreter.xref.InterpreterContext;
import org.apache.zeppelin.interpreter.xref.InterpreterException;
import org.apache.zeppelin.interpreter.xref.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.xref.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Disabled(value="Contains external dependencies")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class PySparkInterpreterMatplotlibTest {

  static SparkInterpreter sparkInterpreter;
  static PySparkInterpreter pyspark;
  static InterpreterGroup intpGroup;
  static Logger LOGGER = LoggerFactory.getLogger(PySparkInterpreterTest.class);
  static InterpreterContextImpl context;

  public static class AltPySparkInterpreter extends PySparkInterpreter {
    /**
     * Since pyspark output is  sent to an outputstream rather than
     * being directly provided by interpret(), this subclass is created to
     * override interpret() to append the result from the outputStream
     * for the sake of convenience in testing.
     */
    public AltPySparkInterpreter(Properties property) {
      super(property);
    }

    /**
     * This code is mainly copied from RemoteInterpreterServer.java which
     * normally handles this in real use cases.
     */
    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) throws InterpreterException {
      context.out().clear();
      InterpreterResult result = super.interpret(st, context);
      List<InterpreterResultMessage> resultMessages = null;
      try {
        context.out().flush();
        resultMessages = context.out().toInterpreterResultMessage();
      } catch (IOException e) {
        Assertions.fail("Failure happened: " + e.getMessage());
      }
      resultMessages.addAll(result.message());

      return new InterpreterResult(result.code(), resultMessages);
    }
  }

  private static Properties getPySparkTestProperties() throws IOException {
    Properties p = new Properties();
    p.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local[*]");
    p.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "Zeppelin Test");
    p.setProperty("zeppelin.spark.useHiveContext", "true");
    p.setProperty("zeppelin.spark.maxResult", "1000");
    p.setProperty("zeppelin.spark.importImplicit", "true");
    p.setProperty("zeppelin.pyspark.python", "python");
    p.setProperty("zeppelin.pyspark.useIPython", "false");
    p.setProperty("zeppelin.python.gatewayserver_address", "127.0.0.1");
    p.setProperty("zeppelin.spark.deprecatedMsg.show", "false");
    return p;
  }

  @BeforeAll
  public static void setUp() throws Exception {
    intpGroup = new InterpreterGroupImpl();
    intpGroup.put("note", new LinkedList<>());
    context = InterpreterContextImpl.builder()
        .setNoteId("note")
        .setInterpreterOut(new InterpreterOutputImpl())
        .setIntpEventClient(mock(RemoteInterpreterEventClientImpl.class))
        .setAngularObjectRegistry(new AngularObjectRegistryImpl(intpGroup.getId(), null))
        .build();
    InterpreterContextStore.set(context);

    sparkInterpreter = new SparkInterpreter(getPySparkTestProperties());
    intpGroup.get("note").add(sparkInterpreter);
    sparkInterpreter.setInterpreterGroup(intpGroup);
    sparkInterpreter.open();

    pyspark = new AltPySparkInterpreter(getPySparkTestProperties());
    intpGroup.get("note").add(pyspark);
    pyspark.setInterpreterGroup(intpGroup);
    pyspark.open();
  }

  @AfterAll
  public static void tearDown() throws InterpreterException {
    pyspark.close();
    sparkInterpreter.close();
  }

  @Test
  void dependenciesAreInstalled() throws InterpreterException {
    // matplotlib
    InterpreterResult ret = pyspark.interpret("import matplotlib", context);
    assertEquals(Code.SUCCESS, ret.code(), ret.message().toString());

    // inline backend
    ret = pyspark.interpret("import backend_zinline", context);
    assertEquals(Code.SUCCESS, ret.code(), ret.message().toString());
  }

  @Test
  void showPlot() throws InterpreterException {
    // Simple plot test
    InterpreterResult ret;
    ret = pyspark.interpret("import matplotlib.pyplot as plt", context);
    ret = pyspark.interpret("plt.close()", context);
    ret = pyspark.interpret("z.configure_mpl(interactive=False)", context);
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret = pyspark.interpret("plt.show()", context);

    assertEquals(Code.SUCCESS, ret.code(), ret.message().toString());
    assertEquals(Type.HTML, ret.message().get(0).getType(), ret.message().toString());
    assertTrue(ret.message().get(0).getData().contains("data:image/png;base64"));
    assertTrue(ret.message().get(0).getData().contains("<div>"));
  }

  @Test
  // Test for when configuration is set to auto-close figures after show().
  void testClose() throws InterpreterException {
    InterpreterResult ret;
    InterpreterResult ret1;
    InterpreterResult ret2;
    ret = pyspark.interpret("import matplotlib.pyplot as plt", context);
    ret = pyspark.interpret("plt.close()", context);
    ret = pyspark.interpret("z.configure_mpl(interactive=False, close=True, angular=False)", context);
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret1 = pyspark.interpret("plt.show()", context);

    // Second call to show() should print nothing, and Type should be TEXT.
    // This is because when close=True, there should be no living instances
    // of FigureManager, causing show() to return before setting the output
    // type to HTML.
    ret = pyspark.interpret("plt.show()", context);
    assertEquals(0, ret.message().size());

    // Now test that new plot is drawn. It should be identical to the
    // previous one.
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret2 = pyspark.interpret("plt.show()", context);
    assertEquals(ret1.message().get(0).getType(), ret2.message().get(0).getType());
    assertEquals(ret1.message().get(0).getData(), ret2.message().get(0).getData());
  }

  @Test
  // Test for when configuration is set to not auto-close figures after show().
  void testNoClose() throws InterpreterException {
    InterpreterResult ret;
    InterpreterResult ret1;
    InterpreterResult ret2;
    ret = pyspark.interpret("import matplotlib.pyplot as plt", context);
    ret = pyspark.interpret("plt.close()", context);
    ret = pyspark.interpret("z.configure_mpl(interactive=False, close=False, angular=False)", context);
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret1 = pyspark.interpret("plt.show()", context);

    // Second call to show() should print nothing, and Type should be HTML.
    // This is because when close=False, there should be living instances
    // of FigureManager, causing show() to set the output
    // type to HTML even though the figure is inactive.
    ret = pyspark.interpret("plt.show()", context);
    assertEquals(Code.SUCCESS, ret.code(), ret.message().toString());

    // Now test that plot can be reshown if it is updated. It should be
    // different from the previous one because it will plot the same line
    // again but in a different color.
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret2 = pyspark.interpret("plt.show()", context);
    assertEquals(0, ret2.message().size());
  }

  @Test
  // Test angular mode
  void testAngular() throws InterpreterException {
    InterpreterResult ret;
    ret = pyspark.interpret("import matplotlib.pyplot as plt", context);
    ret = pyspark.interpret("plt.close()", context);
    ret = pyspark.interpret("z.configure_mpl(interactive=False, close=False, angular=True)", context);
    ret = pyspark.interpret("plt.plot([1, 2, 3])", context);
    ret = pyspark.interpret("plt.show()", context);
    assertEquals(Code.SUCCESS, ret.code(), ret.message().toString());
    assertEquals(Type.ANGULAR, ret.message().get(0).getType(), ret.message().toString());

    // Check if the figure data is in the Angular Object Registry
    AngularObjectRegistry registry = context.getAngularObjectRegistry();
    String figureData = registry.getAll("note", null).get(0).toString();
    assertTrue(figureData.contains("data:image/png;base64"));
  }
}
