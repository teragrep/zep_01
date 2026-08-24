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

package com.teragrep.zep_01.interpreter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public final class ConfInterpreterTest extends AbstractInterpreterTest {

  private ExecutionContext executionContext = new ExecutionContext("user1", "note1", "test");

  @Test
  public void testCorrectConf(){
    // Properties should exist in the Interpreters setting before they are updated by ConfInterpreter.
    final InterpreterSetting testSetting = interpreterSettingManager.getByName("test");
    testSetting.setProperty("property_1","default_value");
    testSetting.setProperty("new_property","default_value");

    final Interpreter confInterpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test.conf", executionContext));

    final InterpreterContext context = InterpreterContext.builder()
              .setNoteId("noteId")
              .setParagraphId("paragraphId")
              .build();


    final InterpreterResult confResult = Assertions.assertDoesNotThrow(()->confInterpreter.interpret("property_1\tnew_value\nnew_property\tdummy_value", context));
    Assertions.assertEquals(InterpreterResult.Code.SUCCESS, confResult.code);

    final Interpreter interpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test", executionContext));
    final InterpreterResult result = Assertions.assertDoesNotThrow(()->interpreter.interpret("hello world",context));
    Assertions.assertEquals(InterpreterResult.Code.SUCCESS, result.code);

    Assertions.assertEquals(6, interpreter.getProperties().size());
    Assertions.assertEquals("new_value", interpreter.getProperty("property_1"));
    Assertions.assertEquals("dummy_value", interpreter.getProperty("new_property"));
    Assertions.assertEquals("value_3", interpreter.getProperty("property_3"));

    // rerun the paragraph with the same properties would result in SUCCESS
    final InterpreterResult confResult2 = Assertions.assertDoesNotThrow(()->confInterpreter.interpret("property_1\tnew_value\nnew_property\tdummy_value", context));
    Assertions.assertEquals(InterpreterResult.Code.SUCCESS, confResult2.code);

    // trying to modify properties after interpreter is running would result in ERROR
    final InterpreterResult confResult3 = Assertions.assertDoesNotThrow(()->confInterpreter.interpret("property_1\tnew_value_2\nnew_property\tdummy_value", context));
    Assertions.assertEquals(InterpreterResult.Code.ERROR, confResult3.code);
  }

  @Test
  public void testEmptyConf(){
    final Interpreter confInterpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test.conf", executionContext));

    final InterpreterContext context = InterpreterContext.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .build();
    final InterpreterResult result = Assertions.assertDoesNotThrow(()->confInterpreter.interpret("", context));
    Assertions.assertEquals(InterpreterResult.Code.SUCCESS, result.code);

    final Interpreter interpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test", executionContext));
    Assertions.assertEquals(5, interpreter.getProperties().size());
    Assertions.assertEquals("value_1", interpreter.getProperty("property_1"));
    Assertions.assertEquals("value_3", interpreter.getProperty("property_3"));
  }


  @Test
  public void testRunningAfterOtherInterpreter(){
    // Properties should exist in the Interpreters setting before they are updated by ConfInterpreter.
    final InterpreterSetting testSetting = interpreterSettingManager.getByName("test");
    testSetting.setProperty("property_1","default_value");
    testSetting.setProperty("new_property","default_value");

    final Interpreter confInterpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test.conf", executionContext));


    final InterpreterContext context = InterpreterContext.builder()
              .setNoteId("noteId")
              .setParagraphId("paragraphId")
              .build();

    final Interpreter interpreter = Assertions.assertDoesNotThrow(()->interpreterFactory.getInterpreter("test", executionContext));
    InterpreterResult result = Assertions.assertDoesNotThrow(()->interpreter.interpret("hello world",context));
    Assertions.assertEquals(InterpreterResult.Code.SUCCESS, result.code);

    final InterpreterResult confResult = Assertions.assertDoesNotThrow(()->confInterpreter.interpret("property_1\tnew_value\nnew_property\tdummy_value", context));
    Assertions.assertEquals(InterpreterResult.Code.ERROR, confResult.code);
  }

  @Test
  public void testAddUndefinedProperty() throws InterpreterException {
    final String key = "unexpected_key";
    final String value = "nefarious_value";
    final ConfInterpreter confInterpreter = (ConfInterpreter) interpreterFactory.getInterpreter("test.conf", executionContext);
    final InterpreterContext context = InterpreterContext.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .build();
    final InterpreterResult result = Assertions.assertDoesNotThrow(()->confInterpreter.interpret(key + "\t" + value, context));
    Assertions.assertEquals(InterpreterResult.Code.ERROR,result.code());
    Assertions.assertTrue(result.message().get(0).getData().contains("Tried to add an unknown key to Interpreter's properties: "+key+" Please make sure that the key is listed as a property in the Interpreters page"));
  }
}
