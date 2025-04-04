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

package com.teragrep.zep_01.python;

import net.jodah.concurrentunit.Waiter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterGroup;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.LazyOpenInterpreter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This test class is also used in spark interpreter module
 *
 * @author pdallig
 */
@SuppressWarnings("java:S5786")
public class PythonInterpreterTest extends BasePythonInterpreterTest {

  @Override
  @BeforeEach
  public void setUp() throws InterpreterException {

    intpGroup = new InterpreterGroup();

    Properties properties = new Properties();
    properties.setProperty("zeppelin.python.maxResult", "3");
    properties.setProperty("zeppelin.python.useIPython", "false");
    properties.setProperty("zeppelin.python.gatewayserver_address", "127.0.0.1");

    interpreter = new LazyOpenInterpreter(new PythonInterpreter(properties));

    intpGroup.put("note", new LinkedList<Interpreter>());
    intpGroup.get("note").add(interpreter);
    interpreter.setInterpreterGroup(intpGroup);

    InterpreterContext.set(getInterpreterContext());
    interpreter.open();
  }

  @Override
  @AfterEach
  public void tearDown() throws InterpreterException {
    intpGroup.close();
  }

  @Override
  @Test
  public void testCodeCompletion() throws InterpreterException, IOException, InterruptedException {
    super.testCodeCompletion();

    //TODO(zjffdu) PythonInterpreter doesn't support this kind of code completion for now.
    // completion
    //    InterpreterContext context = getInterpreterContext();
    //    List<InterpreterCompletion> completions = interpreter.completion("ab", 2, context);
    //    assertEquals(2, completions.size());
    //    assertEquals("abc", completions.get(0).getValue());
    //    assertEquals("abs", completions.get(1).getValue());
  }

  private class infinityPythonJob implements Runnable {
    @Override
    public void run() {
      String code = "import time\nwhile True:\n  time.sleep(1)";
      InterpreterResult ret = null;
      try {
        ret = interpreter.interpret(code, getInterpreterContext());
      } catch (InterpreterException e) {
        Assertions.fail("Failure happened: " + e.getMessage());
      }
      assertNotNull(ret);
      Pattern expectedMessage = Pattern.compile("KeyboardInterrupt");
      Matcher m = expectedMessage.matcher(ret.message().toString());
      assertTrue(m.find());
    }
  }

  @Disabled(value="This was disabled to begin with")
  @Test
  void testCancelIntp() throws InterruptedException, InterpreterException {
    assertEquals(InterpreterResult.Code.SUCCESS,
        interpreter.interpret("a = 1\n", getInterpreterContext()).code());
    Thread t = new Thread(new infinityPythonJob());
    t.start();
    Thread.sleep(5000);
    interpreter.cancel(getInterpreterContext());
    assertTrue(t.isAlive());
    t.join(2000);
    assertFalse(t.isAlive());
  }

  @Disabled(value="Contains sleep")
  @Test
  void testPythonProcessKilled() throws InterruptedException, TimeoutException {
    final Waiter waiter = new Waiter();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          InterpreterResult result = interpreter.interpret("import time\ntime.sleep(1000)",
                  getInterpreterContext());
          waiter.assertEquals(InterpreterResult.Code.ERROR, result.code());
          waiter.assertEquals(
                  "Python process is abnormally exited, please check your code and log.",
                  result.message().get(0).getData());
        } catch (InterpreterException e) {
          waiter.fail("Should not throw exception\n" + ExceptionUtils.getStackTrace(e));
        }
        waiter.resume();
      }
    };
    thread.start();
    Thread.sleep(3000);
    PythonInterpreter pythonInterpreter = (PythonInterpreter)
            ((LazyOpenInterpreter) interpreter).getInnerInterpreter();
    pythonInterpreter.getPythonProcessLauncher().stop();
    waiter.await(3000);
  }

  @Test
  public void testFailtoLaunchPythonProcess() throws InterpreterException {
    tearDown();

    intpGroup = new InterpreterGroup();

    Properties properties = new Properties();
    properties.setProperty("zeppelin.python", "invalid_python");
    properties.setProperty("zeppelin.python.useIPython", "false");
    properties.setProperty("zeppelin.python.gatewayserver_address", "127.0.0.1");

    interpreter = new LazyOpenInterpreter(new PythonInterpreter(properties));

    intpGroup.put("note", new LinkedList<Interpreter>());
    intpGroup.get("note").add(interpreter);
    interpreter.setInterpreterGroup(intpGroup);

    InterpreterContext.set(getInterpreterContext());

    try {
      interpreter.interpret("1+1", getInterpreterContext());
      fail("Should fail to open PythonInterpreter");
    } catch (InterpreterException e) {
      String stacktrace = ExceptionUtils.getStackTrace(e);
      assertTrue(stacktrace.contains("No such file or directory"), stacktrace);
    }
  }
}
