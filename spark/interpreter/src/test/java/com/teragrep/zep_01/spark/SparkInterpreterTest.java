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

import org.apache.commons.lang3.StringUtils;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.display.ui.CheckBox;
import com.teragrep.zep_01.display.ui.Password;
import com.teragrep.zep_01.display.ui.Select;
import com.teragrep.zep_01.display.ui.TextBox;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterGroup;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.InterpreterOutputListener;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessageOutput;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterEventClient;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;


class SparkInterpreterTest {

  private SparkInterpreter interpreter;

  // catch the streaming output in onAppend
  private volatile String output = "";
  // catch the interpreter output in onUpdate
  private InterpreterResultMessageOutput messageOutput;

  private RemoteInterpreterEventClient mockRemoteEventClient;

  @BeforeEach
  public void setUp() {
    mockRemoteEventClient = mock(RemoteInterpreterEventClient.class);
  }

  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testSparkInterpreter() throws IOException, InterruptedException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("zeppelin.spark.uiWebUrl", "fake_spark_weburl/{{applicationId}}");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    InterpreterContext context = InterpreterContext.builder()
        .setInterpreterOut(new InterpreterOutput())
        .setIntpEventClient(mockRemoteEventClient)
        .setAngularObjectRegistry(new AngularObjectRegistry("spark", null))
        .build();
    InterpreterContext.set(context);

    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    interpreter.open();

    InterpreterResult result = interpreter.interpret("val a=\"hello world\"", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // Use contains instead of equals, because there's behavior difference between different scala versions
    assertTrue(output.contains("a: String = hello world\n"), output);

    result = interpreter.interpret("print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("hello world", output);

    // java stdout
    result = interpreter.interpret("System.out.print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("hello world", output);

    // incomplete
    result = interpreter.interpret("println(a", getInterpreterContext());
    assertEquals(InterpreterResult.Code.INCOMPLETE, result.code());

    // syntax error
    result = interpreter.interpret("println(b)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.ERROR, result.code());
    assertTrue(output.contains("not found: value b"));

    // multiple line
    result = interpreter.interpret("\"123\".\ntoInt", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // single line comment
    result = interpreter.interpret("print(\"hello world\")/*comment here*/", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("hello world", output);

    result = interpreter.interpret("/*comment here*/\nprint(\"hello world\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    if (!interpreter.isScala213()) {
      // multiple line comment, not supported by scala-2.13
      context = getInterpreterContext();
      result = interpreter.interpret("/*line 1 \n line 2*/", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code(), context.out.toString());
    }

    // test function
    result = interpreter.interpret("def add(x:Int, y:Int)\n{ return x+y }", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("print(add(1,2))", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("/*line 1 \n line 2*/print(\"hello world\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // Companion object with case class
    result = interpreter.interpret("import scala.math._\n" +
        "object Circle {\n" +
        "  private def calculateArea(radius: Double): Double = Pi * pow(radius, 2.0)\n" +
        "}\n" +
        "case class Circle(radius: Double) {\n" +
        "  import Circle._\n" +
        "  def area: Double = calculateArea(radius)\n" +
        "}\n" +
        "\n" +
        "val circle1 = new Circle(5.0)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // use case class in spark
    //    context = getInterpreterContext();
    //    result = interpreter.interpret("sc\n.range(1, 10)\n.map(e=>Circle(e))\n.collect()", context);
    //    assertEquals(context.out.toString(), InterpreterResult.Code.SUCCESS, result.code());

    // class extend
    result = interpreter.interpret("import java.util.ArrayList", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("class MyArrayList extends ArrayList{}", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark rdd operation
    context = getInterpreterContext();
    context.setParagraphId("pid_1");
    result = interpreter.interpret("sc\n.range(1, 10)\n.map(e=>e)\n.sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(output.contains("45"));
    ArgumentCaptor<Map> captorEvent = ArgumentCaptor.forClass(Map.class);
    verify(mockRemoteEventClient).onParaInfosReceived(captorEvent.capture());
    assertEquals("pid_1", captorEvent.getValue().get("paraId"));

    reset(mockRemoteEventClient);
    context = getInterpreterContext();
    context.setParagraphId("pid_2");
    result = interpreter.interpret("sc\n.range(1, 10)\n.sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(output.contains("45"));
    captorEvent = ArgumentCaptor.forClass(Map.class);
    verify(mockRemoteEventClient).onParaInfosReceived(captorEvent.capture());
    assertEquals("pid_2", captorEvent.getValue().get("paraId"));

    // spark job url is sent
    ArgumentCaptor<Map> onParaInfosReceivedArg = ArgumentCaptor.forClass(Map.class);
    verify(mockRemoteEventClient).onParaInfosReceived(onParaInfosReceivedArg.capture());
    assertTrue(((String) onParaInfosReceivedArg.getValue().get("jobUrl")).startsWith("fake_spark_weburl/"
            + interpreter.getJavaSparkContext().sc().applicationId()));

    // RDD of case class objects
    result = interpreter.interpret(
        "case class A(a: Integer, b: Integer)\n" +
        "sc.parallelize(Seq(A(10, 20), A(30, 40))).collect()", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // Dataset of case class objects
    result = interpreter.interpret("val bankText = sc.textFile(\"bank.csv\")", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    context = getInterpreterContext();
    result = interpreter.interpret(
        "case class Bank(age:Integer, job:String, marital : String, education : String, balance : Integer)\n" +
            "val bank = bankText.map(s=>s.split(\";\")).filter(s => s(0)!=\"\\\"age\\\"\").map(\n" +
            "    s => Bank(s(0).toInt, \n" +
            "            s(1).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(2).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(3).replaceAll(\"\\\"\", \"\"),\n" +
            "            s(5).replaceAll(\"\\\"\", \"\").toInt\n" +
            "        )\n" +
            ").toDF()", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code(), context.out.toString());

    // spark version
    result = interpreter.interpret("sc.version", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark sql test
    String version = output.trim();
    // create dataset from case class
    context = getInterpreterContext();
    result = interpreter.interpret("case class Person(id:Int, name:String, age:Int, country:String)\n" +
            "val df2 = spark.createDataFrame(Seq(Person(1, \"andy\", 20, \"USA\"), " +
            "Person(2, \"jeff\", 23, \"China\"), Person(3, \"james\", 18, \"USA\")))\n" +
            "df2.printSchema\n" +
            "df2.show() ", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("spark", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret(
        "val df = spark.createDataFrame(Seq((1,\"a\"),(2, null)))\n" +
            "df.show()", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // SPARK-43063 changed the output of null to NULL
    assertTrue(StringUtils.containsIgnoreCase(output,
        "+---+----+\n" +
        "| _1|  _2|\n" +
        "+---+----+\n" +
        "|  1|   a|\n" +
        "|  2|null|\n" +
        "+---+----+"));

    // ZeppelinContext
    context = getInterpreterContext();
    result = interpreter.interpret("z.show(df)", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code(), context.out.toString());
    assertEquals(InterpreterResult.Type.TABLE, messageOutput.getType());
    messageOutput.flush();
    assertEquals("_1\t_2\n1\ta\n2\tnull\n", messageOutput.toInterpreterResultMessage().getData());

    context = getInterpreterContext();
    result = interpreter.interpret("z.input(\"name\", \"default_name\")", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(TextBox.class, context.getGui().getForms().get("name"));
    TextBox textBox = (TextBox) context.getGui().getForms().get("name");
    assertEquals("name", textBox.getName());
    assertEquals("default_name", textBox.getDefaultValue());

    context = getInterpreterContext();
    result = interpreter.interpret("z.password(\"pwd\")", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(Password.class, context.getGui().getForms().get("pwd"));
    Password pwd = (Password) context.getGui().getForms().get("pwd");
    assertEquals("pwd", pwd.getName());

    context = getInterpreterContext();
    result = interpreter.interpret("z.checkbox(\"checkbox_1\", Seq((\"value_1\", \"name_1\"), (\"value_2\", \"name_2\")), Seq(\"value_2\"))", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(CheckBox.class, context.getGui().getForms().get("checkbox_1"));
    CheckBox checkBox = (CheckBox) context.getGui().getForms().get("checkbox_1");
    assertEquals("checkbox_1", checkBox.getName());
    assertEquals(1, checkBox.getDefaultValue().length);
    assertEquals("value_2", checkBox.getDefaultValue()[0]);
    assertEquals(2, checkBox.getOptions().length);
    assertEquals("value_1", checkBox.getOptions()[0].getValue());
    assertEquals("name_1", checkBox.getOptions()[0].getDisplayName());
    assertEquals("value_2", checkBox.getOptions()[1].getValue());
    assertEquals("name_2", checkBox.getOptions()[1].getDisplayName());

    context = getInterpreterContext();
    result = interpreter.interpret("z.select(\"select_1\", Seq((\"value_1\", \"name_1\"), (\"value_2\", \"name_2\")), Seq(\"value_2\"))", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(Select.class, context.getGui().getForms().get("select_1"));
    Select select = (Select) context.getGui().getForms().get("select_1");
    assertEquals("select_1", select.getName());
    // TODO(zjffdu) it seems a bug of GUI, the default value should be 'value_2', but it is List(value_2)
    // assertEquals("value_2", select.getDefaultValue());
    assertEquals(2, select.getOptions().length);
    assertEquals("value_1", select.getOptions()[0].getValue());
    assertEquals("name_1", select.getOptions()[0].getDisplayName());
    assertEquals("value_2", select.getOptions()[1].getValue());
    assertEquals("name_2", select.getOptions()[1].getDisplayName());

    // completions
    List<InterpreterCompletion> completions = interpreter.completion("a.", 2, getInterpreterContext());
    assertTrue(completions.size() > 0);

    completions = interpreter.completion("a.isEm", 6, getInterpreterContext());
    assertEquals(1, completions.size());
    assertEquals("isEmpty", completions.get(0).name);

    completions = interpreter.completion("sc.ra", 5, getInterpreterContext());
    assertEquals(1, completions.size());
    assertEquals("range", completions.get(0).name);

    // cursor in middle of code
    completions = interpreter.completion("sc.ra\n1+1", 5, getInterpreterContext());
    assertEquals(1, completions.size());
    assertEquals("range", completions.get(0).name);

    if (!interpreter.isScala213()) {
      // Zeppelin-Display
      result = interpreter.interpret("import com.teragrep.zep_01.display.angular.notebookscope._\n" +
              "import AngularElem._", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());

      context = getInterpreterContext();
      result = interpreter.interpret("<div style=\"color:blue\">\n" +
              "<h4>Hello Angular Display System</h4>\n" +
              "</div>.display", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code(), context.out.toString());
      assertEquals(InterpreterResult.Type.ANGULAR, messageOutput.getType());
      assertTrue(messageOutput.toInterpreterResultMessage().getData().contains("Hello Angular Display System"));

      result = interpreter.interpret("<div class=\"btn btn-success\">\n" +
              "  Click me\n" +
              "</div>.onClick{() =>\n" +
              "  println(\"hello world\")\n" +
              "}.display", getInterpreterContext());
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(InterpreterResult.Type.ANGULAR, messageOutput.getType());
      assertTrue(messageOutput.toInterpreterResultMessage().getData().contains("Click me"));
    }

    // getProgress
    final InterpreterContext context2 = getInterpreterContext();
    Thread interpretThread = new Thread() {
      @Override
      public void run() {
        InterpreterResult result = null;
        try {
          result = interpreter.interpret(
              "val df = sc.parallelize(1 to 10, 5).foreach(e=>Thread.sleep(1000))", context2);
        } catch (InterpreterException e) {
          Assertions.fail("Failure happened: " + e.getMessage());
        }
        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      }
    };
    interpretThread.start();
    boolean nonZeroProgress = false;
    int progress = 0;
    while (interpretThread.isAlive()) {
      progress = interpreter.getProgress(context2);
      assertTrue(progress >= 0);
      if (progress != 0 && progress != 100) {
        nonZeroProgress = true;
      }
      Thread.sleep(100);
    }
    assertTrue(nonZeroProgress);

    // cancel
    final InterpreterContext context3 = getInterpreterContext();
    interpretThread = new Thread() {
      @Override
      public void run() {
        InterpreterResult result = null;
        try {
          result = interpreter.interpret(
              "val df = sc.parallelize(1 to 10, 2).foreach(e=>Thread.sleep(1000))", context3);
        } catch (InterpreterException e) {
          Assertions.fail("Failure happened: " + e.getMessage());
        }
        assertEquals(InterpreterResult.Code.ERROR, result.code());
        assertTrue(output.contains("cancelled"));
      }
    };

    interpretThread.start();
    // sleep 1 second to wait for the spark job start
    Thread.sleep(1000);
    interpreter.cancel(context3);
    interpretThread.join();
  }

  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testDisableReplOutput() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("zeppelin.spark.printREPLOutput", "false");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    InterpreterContext.set(getInterpreterContext());
    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    interpreter.open();

    InterpreterResult result = interpreter.interpret("val a=\"hello world\"", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // no output for define new variable
    assertEquals("", output);

    result = interpreter.interpret("print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // output from print statement will still be displayed
    assertEquals("hello world", output);
  }

  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testDisableReplOutputForParagraph() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("spark.master", "local");
    properties.setProperty("spark.app.name", "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("zeppelin.spark.printREPLOutput", "true");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    InterpreterContext.set(getInterpreterContext());
    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    interpreter.open();

    InterpreterResult result = interpreter.interpret("val a=\"hello world\"", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // Use contains instead of equals, because there's behavior different between different scala versions
    assertTrue(output.contains("a: String = hello world\n"), output);

    result = interpreter.interpret("print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // output from print statement will still be displayed
    assertEquals("hello world", output);

    // disable REPL output
    InterpreterContext context = getInterpreterContext();
    context.getLocalProperties().put("printREPLOutput", "false");
    result = interpreter.interpret("print(a)", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // output from print statement will disappear
    assertEquals("", output);

    // REPL output get back if we don't set printREPLOutput in paragraph local properties
    result = interpreter.interpret("val a=\"hello world\"", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(output.contains("a: String = hello world\n"), output);

    result = interpreter.interpret("print(a)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    // output from print statement will still be displayed
    assertEquals("hello world", output);
  }

  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testSchedulePool() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("spark.scheduler.mode", "FAIR");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    InterpreterContext.set(getInterpreterContext());
    interpreter.open();

    InterpreterContext context = getInterpreterContext();
    context.getLocalProperties().put("pool", "pool1");
    InterpreterResult result = interpreter.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("pool1", interpreter.getSparkContext().getLocalProperty("spark.scheduler.pool"));

    // pool is reset to null if user don't specify it via paragraph properties
    result = interpreter.interpret("sc.range(1, 10).sum", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertNull(interpreter.getSparkContext().getLocalProperty("spark.scheduler.pool"));
  }

  // spark.ui.enabled: false
  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testDisableSparkUI_1() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("spark.ui.enabled", "false");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    InterpreterContext.set(getInterpreterContext());
    interpreter.open();

    InterpreterContext context = getInterpreterContext();
    InterpreterResult result = interpreter.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark job url is not sent
    verify(mockRemoteEventClient, never()).onParaInfosReceived(any(Map.class));
  }

  // zeppelin.spark.ui.hidden: true
  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testDisableSparkUI_2() throws InterpreterException {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("zeppelin.spark.ui.hidden", "true");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    interpreter = new SparkInterpreter(properties);
    interpreter.setInterpreterGroup(mock(InterpreterGroup.class));
    InterpreterContext.set(getInterpreterContext());
    interpreter.open();

    InterpreterContext context = getInterpreterContext();
    InterpreterResult result = interpreter.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    // spark job url is not sent
    verify(mockRemoteEventClient, never()).onParaInfosReceived(any(Map.class));
  }

  @Test
  @Disabled(value="Won't build because it depends on Spark212 being available")
  void testScopedMode() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    // disable color output for easy testing
    properties.setProperty("zeppelin.spark.scala.color", "false");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    SparkInterpreter interpreter1 = new SparkInterpreter(properties);
    SparkInterpreter interpreter2 = new SparkInterpreter(properties);

    InterpreterGroup interpreterGroup = new InterpreterGroup();
    interpreter1.setInterpreterGroup(interpreterGroup);
    interpreter2.setInterpreterGroup(interpreterGroup);

    interpreterGroup.addInterpreterToSession(interpreter1, "session_1");
    interpreterGroup.addInterpreterToSession(interpreter2, "session_2");

    InterpreterContext.set(getInterpreterContext());
    interpreter1.open();
    interpreter2.open();

    // check if there is any duplicated loaded class
    assertSame(interpreter1.getInnerInterpreter().getClass(), interpreter2.getInnerInterpreter().getClass());

    InterpreterContext context = getInterpreterContext();

    InterpreterResult result1 = interpreter1.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result1.code());

    InterpreterResult result2 = interpreter2.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result2.code());

    // interpreter2 continue to work after interpreter1 is closed
    interpreter1.close();

    result2 = interpreter2.interpret("sc.range(1, 10).sum", context);
    assertEquals(InterpreterResult.Code.SUCCESS, result2.code());
    interpreter2.close();
  }

  @AfterEach
  public void tearDown() throws InterpreterException {
    if (this.interpreter != null) {
      this.interpreter.close();
    }
    SparkShims.reset();
  }

  private InterpreterContext getInterpreterContext() {
    output = "";
    InterpreterContext context = InterpreterContext.builder()
        .setInterpreterOut(new InterpreterOutput())
        .setIntpEventClient(mockRemoteEventClient)
        .setAngularObjectRegistry(new AngularObjectRegistry("spark", null))
        .build();
    context.out =
        new InterpreterOutput(

            new InterpreterOutputListener() {
              @Override
              public void onUpdateAll(InterpreterOutput out) {

              }

              @Override
              public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
                try {
                  output = out.toInterpreterResultMessage().getData();
                } catch (IOException e) {
                  Assertions.fail("Failure happened: " + e.getMessage());
                }
              }

              @Override
              public void onUpdate(int index, InterpreterResultMessageOutput out) {
                messageOutput = out;
              }
            });
    return context;
  }
}
