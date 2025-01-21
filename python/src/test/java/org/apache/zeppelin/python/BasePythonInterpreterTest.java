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

package org.apache.zeppelin.python;

import net.jodah.concurrentunit.ConcurrentTestCase;
import org.apache.zeppelin.display.ui.CheckBox;
import org.apache.zeppelin.display.ui.Password;
import org.apache.zeppelin.display.ui.Select;
import org.apache.zeppelin.display.ui.TextBox;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class BasePythonInterpreterTest extends ConcurrentTestCase {

  protected InterpreterGroup intpGroup;
  protected Interpreter interpreter;
  protected boolean isPython2;

  @BeforeEach
  public abstract void setUp() throws InterpreterException;

  @AfterEach
  public abstract void tearDown() throws InterpreterException;

  @Test
  public void testPythonBasics() throws InterpreterException, InterruptedException, IOException {

    InterpreterContext context = getInterpreterContext();
    InterpreterResult result =
        interpreter.interpret("import sys\nprint(sys.version[0])", context);
    assertEquals(Code.SUCCESS, result.code());
    Thread.sleep(100);
    List<InterpreterResultMessage> interpreterResultMessages =
        context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());

    // single output without print
    context = getInterpreterContext();
    result = interpreter.interpret("'hello world'", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("'hello world'", interpreterResultMessages.get(0).getData().trim());

    // unicode
    context = getInterpreterContext();
    result = interpreter.interpret("print(u'你好')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("你好\n", interpreterResultMessages.get(0).getData());

    // only the last statement is printed
    context = getInterpreterContext();
    result = interpreter.interpret("'hello world'\n'hello world2'", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("'hello world2'", interpreterResultMessages.get(0).getData().trim());

    // single output
    context = getInterpreterContext();
    result = interpreter.interpret("print('hello world')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("hello world\n", interpreterResultMessages.get(0).getData());

    // multiple output
    context = getInterpreterContext();
    result = interpreter.interpret("print('hello world')\nprint('hello world2')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("hello world\nhello world2\n", interpreterResultMessages.get(0).getData());

    // assignment
    context = getInterpreterContext();
    result = interpreter.interpret("abc=1", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(0, interpreterResultMessages.size());

    // if block
    context = getInterpreterContext();
    result =
        interpreter.interpret("if abc > 0:\n\tprint('True')\nelse:\n\tprint('False')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("True\n", interpreterResultMessages.get(0).getData());

    // for loop
    context = getInterpreterContext();
    result = interpreter.interpret("for i in range(3):\n\tprint(i)", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("0\n1\n2\n", interpreterResultMessages.get(0).getData());

    // syntax error
    context = getInterpreterContext();
    result = interpreter.interpret("print(unknown)", context);
    Thread.sleep(100);
    assertEquals(Code.ERROR, result.code());
    assertTrue(result.message().get(0).getData().contains("name 'unknown' is not defined"));

    // raise runtime exception
    context = getInterpreterContext();
    result = interpreter.interpret("1/0", context);
    Thread.sleep(100);
    assertEquals(Code.ERROR, result.code());
    assertTrue(result.message().get(0).getData().contains("ZeroDivisionError"));

    // ZEPPELIN-1133
    context = getInterpreterContext();
    result = interpreter.interpret(
        "from __future__ import print_function\n" +
            "def greet(name):\n" +
            "    print('Hello', name)\n" +
            "greet('Jack')",
        context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("Hello Jack\n", interpreterResultMessages.get(0).getData());

    // ZEPPELIN-1114
    context = getInterpreterContext();
    result = interpreter.interpret("print('there is no Error: ok')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("there is no Error: ok\n", interpreterResultMessages.get(0).getData());

    // ZEPPELIN-3687
    context = getInterpreterContext();
    result = interpreter.interpret("# print('Hello')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(0, interpreterResultMessages.size());

    context = getInterpreterContext();
    result = interpreter.interpret(
        "# print('Hello')\n# print('How are u?')\n# time.sleep(1)", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(0, interpreterResultMessages.size());

    // multiple text output
    context = getInterpreterContext();
    result = interpreter.interpret(
        "for i in range(1,4):\n" + "\tprint(i)", context);
    assertEquals(Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    assertEquals(1, interpreterResultMessages.size());
    assertEquals("1\n2\n3\n", interpreterResultMessages.get(0).getData());
  }

  @Test
  public void testCodeCompletion() throws InterpreterException, IOException, InterruptedException {
    // define `a` first
    InterpreterContext context = getInterpreterContext();
    String st = "a='hello'";
    InterpreterResult result = interpreter.interpret(st, context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());

    // now we can get the completion for `a.`
    context = getInterpreterContext();
    st = "a.";
    List<InterpreterCompletion> completions = interpreter.completion(st, st.length(), context);
    // it is different for python2 and python3 and may even different for different minor version
    // so only verify it is larger than 20
    assertTrue(completions.size() > 20);

    context = getInterpreterContext();
    st = "a.co";
    completions = interpreter.completion(st, st.length(), context);
    assertEquals(1, completions.size());
    assertEquals("count", completions.get(0).getValue());

    // cursor is in the middle of code
    context = getInterpreterContext();
    st = "a.co\b='hello";
    completions = interpreter.completion(st, 4, context);
    assertEquals(1, completions.size());
    assertEquals("count", completions.get(0).getValue());
  }

  @Test
  public void testZeppelinContext() throws InterpreterException, InterruptedException, IOException {
    // TextBox
    InterpreterContext context = getInterpreterContext();
    InterpreterResult result =
        interpreter.interpret("z.input(name='text_1', defaultValue='value_1')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    List<InterpreterResultMessage> interpreterResultMessages =
        context.out.toInterpreterResultMessage();
    assertTrue(interpreterResultMessages.get(0).getData().contains("'value_1'"));
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(TextBox.class, context.getGui().getForms().get("text_1"));
    TextBox textbox = (TextBox) context.getGui().getForms().get("text_1");
    assertEquals("text_1", textbox.getName());
    assertEquals("value_1", textbox.getDefaultValue());

    // Password
    context = getInterpreterContext();
    result =
        interpreter.interpret("z.password(name='pwd_1')", context);
    Thread.sleep(100);
    assertEquals(Code.SUCCESS, result.code());
    assertInstanceOf(Password.class, context.getGui().getForms().get("pwd_1"));
    Password password = (Password) context.getGui().getForms().get("pwd_1");
    assertEquals("pwd_1", password.getName());

    // Select
    context = getInterpreterContext();
    result = interpreter.interpret("z.select(name='select_1'," +
        " options=[('value_1', 'name_1'), ('value_2', 'name_2')])", context);
    assertEquals(Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(Select.class, context.getGui().getForms().get("select_1"));
    Select select = (Select) context.getGui().getForms().get("select_1");
    assertEquals("select_1", select.getName());
    assertEquals(2, select.getOptions().length);
    assertEquals("name_1", select.getOptions()[0].getDisplayName());
    assertEquals("value_1", select.getOptions()[0].getValue());

    // CheckBox
    context = getInterpreterContext();
    result = interpreter.interpret("z.checkbox(name='checkbox_1'," +
        "options=[('value_1', 'name_1'), ('value_2', 'name_2')])", context);
    assertEquals(Code.SUCCESS, result.code());
    assertEquals(1, context.getGui().getForms().size());
    assertInstanceOf(CheckBox.class, context.getGui().getForms().get("checkbox_1"));
    CheckBox checkbox = (CheckBox) context.getGui().getForms().get("checkbox_1");
    assertEquals("checkbox_1", checkbox.getName());
    assertEquals(2, checkbox.getOptions().length);
    assertEquals("name_1", checkbox.getOptions()[0].getDisplayName());
    assertEquals("value_1", checkbox.getOptions()[0].getValue());
    
    // clear output
    context = getInterpreterContext();
    result = interpreter.interpret("import time\nprint(\"Hello\")\n" +
        "time.sleep(0.5)\nz.getInterpreterContext().out().clear()\nprint(\"world\")\n", context);
    assertEquals("%text world\n", context.out.getCurrentOutput().toString());
  }

  @Disabled("Flaky test, need to investigate why it fails")
  @Test
  public void testRedefinitionZeppelinContext() throws InterpreterException {
    String redefinitionCode = "z = 1\n";
    String restoreCode = "z = __zeppelin__\n";
    String validCode = "z.input(\"test\")\n";

    InterpreterContext context = getInterpreterContext();
    InterpreterResult result = interpreter.interpret(validCode, context);
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret(redefinitionCode, context);
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret(validCode, context);
    assertEquals(
            Code.ERROR, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret(restoreCode, context);
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret("type(__zeppelin__)", context);
    System.out.println("result: " + context.out.toString() + ", " + result.toString());
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret("type(z)", context);
    System.out.println("result2: " + context.out.toString() + ", " + result.toString());
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());

    context = getInterpreterContext();
    result = interpreter.interpret(validCode, context);
    assertEquals(
            Code.SUCCESS, result.code(),
        context.out.toString() + ", " + result.toString());
  }

  protected InterpreterContext getInterpreterContext() {
    return InterpreterContext.builder()
        .setNoteId("noteId")
        .setParagraphId("paragraphId")
        .setInterpreterOut(new InterpreterOutput())
        .setIntpEventClient(mock(RemoteInterpreterEventClient.class))
        .build();
  }
}
