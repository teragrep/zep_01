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

import org.apache.zeppelin.display.AngularObjectRegistryImpl;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClientImpl;
import org.apache.zeppelin.interpreter.xref.Code;
import org.apache.zeppelin.interpreter.xref.InterpreterException;
import org.apache.zeppelin.interpreter.xref.Type;
import org.apache.zeppelin.resource.LocalResourcePool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Disabled(value="Won't build because it depends on Spark212 being available, setup fails")
public class SparkSqlInterpreterTest {

  private static SparkSqlInterpreter sqlInterpreter;
  private static SparkInterpreter sparkInterpreter;
  private static InterpreterContextImpl context;
  private static InterpreterGroup intpGroup;

  @BeforeAll
  public static void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local[4]");
    p.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    p.setProperty("zeppelin.spark.maxResult", "10");
    p.setProperty("zeppelin.spark.concurrentSQL", "true");
    p.setProperty("zeppelin.spark.sql.stacktrace", "true");
    p.setProperty("zeppelin.spark.useHiveContext", "true");
    p.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

    intpGroup = new InterpreterGroupImpl();
    sparkInterpreter = new SparkInterpreter(p);
    sparkInterpreter.setInterpreterGroup(intpGroup);

    sqlInterpreter = new SparkSqlInterpreter(p);
    sqlInterpreter.setInterpreterGroup(intpGroup);
    intpGroup.put("session_1", new LinkedList<>());
    intpGroup.get("session_1").add(sparkInterpreter);
    intpGroup.get("session_1").add(sqlInterpreter);

    context = getInterpreterContext();
    InterpreterContextStore.set(context);

    sparkInterpreter.open();
    sqlInterpreter.open();
  }

  private static InterpreterContextImpl getInterpreterContext() {
    return InterpreterContextImpl.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .setParagraphTitle("title")
            .setAngularObjectRegistry(new AngularObjectRegistryImpl(intpGroup.getId(), null))
            .setResourcePool(new LocalResourcePool("id"))
            .setInterpreterOut(new InterpreterOutputImpl())
            .setIntpEventClient(mock(RemoteInterpreterEventClientImpl.class))
            .build();
  }

  @AfterAll
  public static void tearDown() throws InterpreterException {
    sqlInterpreter.close();
    sparkInterpreter.close();
  }

  @Test
  void test() throws InterpreterException, IOException {
    InterpreterResult result = sparkInterpreter.interpret("case class Test(name:String, age:Int)", context);
    assertEquals(Code.SUCCESS, result.code());
    result = sparkInterpreter.interpret("val test = sc.parallelize(Seq(Test(\"moon\\t1\", 33), Test(\"jobs\", 51), Test(\"gates\", 51), Test(\"park\\n1\", 34)))", context);
    assertEquals(Code.SUCCESS, result.code());
    result = sparkInterpreter.interpret("test.toDF.registerTempTable(\"test\")", context);
    assertEquals(Code.SUCCESS, result.code());

    InterpreterResult ret = sqlInterpreter.interpret("select name, age from test where age < 40", context);
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals(Type.TABLE, context.out.toInterpreterResultMessage().get(0).getType());
    assertEquals("name\tage\nmoon 1\t33\npark 1\t34\n", context.out.toInterpreterResultMessage().get(0).getData());

    ret = sqlInterpreter.interpret("select wrong syntax", context);
    assertEquals(Code.ERROR, ret.code());
    assertTrue(context.out.toInterpreterResultMessage().get(0).getData().length() > 0);

    assertEquals(Code.SUCCESS, sqlInterpreter.interpret("select case when name='aa' then name else name end from test", context).code());
  }

  @Test
  void testStruct() throws InterpreterException {
    sparkInterpreter.interpret("case class Person(name:String, age:Int)", context);
    sparkInterpreter.interpret("case class People(group:String, person:Person)", context);
    sparkInterpreter.interpret(
        "val gr = sc.parallelize(Seq(People(\"g1\", Person(\"moon\",33)), People(\"g2\", Person(\"sun\",11))))",
        context);
    sparkInterpreter.interpret("gr.toDF.registerTempTable(\"gr\")", context);

    InterpreterResult ret = sqlInterpreter.interpret("select * from gr", context);
    assertEquals(Code.SUCCESS, ret.code());

  }

  @Test
  public void test_null_value_in_row() throws InterpreterException {
    sparkInterpreter.interpret("import org.apache.spark.sql._", context);
    sparkInterpreter.interpret(
        "import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}",
        context);

    sparkInterpreter.interpret(
        "def toInt(s:String): Any = {try { s.trim().toInt} catch {case e:Exception => null}}",
        context);
    sparkInterpreter.interpret(
        "val schema = StructType(Seq(StructField(\"name\", StringType, false),StructField(\"age\" , IntegerType, true),StructField(\"other\" , StringType, false)))",
        context);
    sparkInterpreter.interpret(
        "val csv = sc.parallelize(Seq((\"jobs, 51, apple\"), (\"gates, , microsoft\")))",
        context);
    sparkInterpreter.interpret(
        "val raw = csv.map(_.split(\",\")).map(p => Row(p(0),toInt(p(1)),p(2)))",
        context);
    sparkInterpreter.interpret("val people = sqlContext.createDataFrame(raw, schema)",
        context);
    sparkInterpreter.interpret("people.toDF.registerTempTable(\"people\")", context);

    InterpreterResult ret = sqlInterpreter.interpret(
        "select name, age from people where name = 'gates'", context);
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals(Type.TABLE, ret.message().get(0).getType());
    assertEquals("name\tage\ngates\tnull\n", ret.message().get(0).getData());
  }

  @Test
  void testMaxResults() throws InterpreterException, IOException {
    sparkInterpreter.interpret("case class P(age:Int)", context);
    sparkInterpreter.interpret(
        "val gr = sc.parallelize(Seq(P(1),P(2),P(3),P(4),P(5),P(6),P(7),P(8),P(9),P(10),P(11)))",
        context);
    sparkInterpreter.interpret("gr.toDF.registerTempTable(\"gr\")", context);

    InterpreterResult ret = sqlInterpreter.interpret("select * from gr", context);
    assertEquals(Code.SUCCESS, ret.code());
    // the number of rows is 10+1, 1 is the head of table
    assertEquals(11, context.out.toInterpreterResultMessage().get(0).getData().split("\n").length);
    assertTrue(context.out.toInterpreterResultMessage().get(1).getData().contains("alert-warning"));

    // test limit local property
    context.getLocalProperties().put("limit", "5");
    ret = sqlInterpreter.interpret("select * from gr", context);
    assertEquals(Code.SUCCESS, ret.code());
    // the number of rows is 5+1, 1 is the head of table
    assertEquals(6, context.out.toInterpreterResultMessage().get(0).getData().split("\n").length);
  }

  @Test
  void testSingleRowResult() throws InterpreterException, IOException {
    sparkInterpreter.interpret("case class P(age:Int)", context);
    sparkInterpreter.interpret(
            "val gr = sc.parallelize(Seq(P(1),P(2),P(3),P(4),P(5),P(6),P(7),P(8),P(9),P(10)))",
            context);
    sparkInterpreter.interpret("gr.toDF.registerTempTable(\"gr\")", context);

    context = InterpreterContextImpl.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .setParagraphTitle("title")
            .setAngularObjectRegistry(new AngularObjectRegistryImpl(intpGroup.getId(), null))
            .setResourcePool(new LocalResourcePool("id"))
            .setInterpreterOut(new InterpreterOutputImpl())
            .setIntpEventClient(mock(RemoteInterpreterEventClientImpl.class))
            .build();
    context.getLocalProperties().put("template", "Total count: <h1>{0}</h1>, Total age: <h1>{1}</h1>");

    InterpreterResult ret = sqlInterpreter.interpret("select count(1), sum(age) from gr", context);
    context.getLocalProperties().remove("template");
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals(Type.HTML, context.out.toInterpreterResultMessage().get(0).getType());
    assertEquals("Total count: <h1>10</h1>, Total age: <h1>55</h1>", context.out.toInterpreterResultMessage().get(0).getData());
  }

  @Test
  void testMultipleStatements() throws InterpreterException, IOException {
    sparkInterpreter.interpret("case class P(age:Int)", context);
    sparkInterpreter.interpret(
            "val gr = sc.parallelize(Seq(P(1),P(2),P(3),P(4)))",
            context);
    sparkInterpreter.interpret("gr.toDF.registerTempTable(\"gr\")", context);

    // Two correct sql
    InterpreterResult ret = sqlInterpreter.interpret(
            "select * --comment_1\nfrom gr;select count(1) from gr", context);
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals(2, context.out.toInterpreterResultMessage().size(), context.out.toString());
    assertEquals(Type.TABLE, context.out.toInterpreterResultMessage().get(0).getType(),
      context.out.toString());
    assertEquals(Type.TABLE, context.out.toInterpreterResultMessage().get(1).getType(),
      context.out.toString());

    // One correct sql + One invalid sql
    ret = sqlInterpreter.interpret("select * from gr;invalid_sql", context);
    assertEquals(Code.ERROR, ret.code());
    assertEquals(2, context.out.toInterpreterResultMessage().size(), context.out.toString());
    assertEquals(Type.TABLE, context.out.toInterpreterResultMessage().get(0).getType(),
      context.out.toString());
    assertTrue(context.out.toString().contains("mismatched input") ||
      context.out.toString().contains("Syntax error"), context.out.toString());

    // One correct sql + One invalid sql + One valid sql (skipped)
    ret = sqlInterpreter.interpret("select * from gr;invalid_sql; select count(1) from gr", context);
    assertEquals(Code.ERROR, ret.code());
    assertEquals(2, context.out.toInterpreterResultMessage().size(), context.out.toString());
    assertEquals(Type.TABLE, context.out.toInterpreterResultMessage().get(0).getType(),
      context.out.toString());
    assertTrue(context.out.toString().contains("mismatched input") ||
      context.out.toString().contains("Syntax error"), context.out.toString());

    // Two 2 comments
    ret = sqlInterpreter.interpret(
            "--comment_1\n--comment_2", context);
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals(0, context.out.toInterpreterResultMessage().size(), context.out.toString());
  }

  @Test
  void testConcurrentSQL() throws InterpreterException, InterruptedException {
    sparkInterpreter.interpret("spark.udf.register(\"sleep\", (e:Int) => {Thread.sleep(e*1000); e})", context);

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        try {
          InterpreterResult result = sqlInterpreter.interpret("select sleep(10)", context);
          assertEquals(Code.SUCCESS, result.code());
        } catch (InterpreterException e) {
          Assertions.fail("Failure happened: " + e.getMessage());
        }
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        try {
          InterpreterResult result = sqlInterpreter.interpret("select sleep(10)", context);
          assertEquals(Code.SUCCESS, result.code());
        } catch (InterpreterException e) {
          Assertions.fail("Failure happened: " + e.getMessage());
        }
      }
    };

    // start running 2 spark sql, each would sleep 10 seconds, the totally running time should
    // be less than 20 seconds, which means they run concurrently.
    long start = System.currentTimeMillis();
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    long end = System.currentTimeMillis();
    assertTrue(((end - start) / 1000) < 20, "running time must be less than 20 seconds");

  }

  @Test
  void testDDL() throws InterpreterException, IOException {
    InterpreterContextImpl context = getInterpreterContext();
    InterpreterResult ret = sqlInterpreter.interpret("create table t1(id int, name string)", context);
    assertEquals(Code.SUCCESS, ret.code(), context.out.toString());
    // spark 1.x will still return DataFrame with non-empty columns.
    // org.apache.spark.sql.DataFrame = [result: string]
    if (!sparkInterpreter.getSparkContext().version().startsWith("1.")) {
      assertTrue(ret.message().isEmpty());
    } else {
      assertEquals(Type.TABLE, ret.message().get(0).getType());
      assertEquals("result\n", ret.message().get(0).getData());
    }

    // create the same table again
    ret = sqlInterpreter.interpret("create table t1(id int, name string)", context);
    assertEquals(Code.ERROR, ret.code());
    assertEquals(1, context.out.toInterpreterResultMessage().size());
    assertEquals(Type.TEXT, context.out.toInterpreterResultMessage().get(0).getType());
    assertTrue(context.out.toInterpreterResultMessage().get(0).getData().contains("already exists"));

    // invalid DDL
    ret = sqlInterpreter.interpret("create temporary function udf1 as 'org.apache.zeppelin.UDF'", context);
    assertEquals(Code.ERROR, ret.code());
    assertEquals(1, context.out.toInterpreterResultMessage().size());
    assertEquals(Type.TEXT, context.out.toInterpreterResultMessage().get(0).getType());
  }
}
