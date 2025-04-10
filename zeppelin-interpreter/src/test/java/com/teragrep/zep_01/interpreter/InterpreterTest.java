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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

//TODO(zjffdu) add more test for Interpreter which is a very important class
public class InterpreterTest {
  private static Logger LOGGER = Logger.getLogger(InterpreterTest.class);

  @Test
  public void testDefaultProperty() {
    Properties p = new Properties();
    p.put("p1", "v1");
    Interpreter intp = new DummyInterpreter(p);

    assertEquals(1, intp.getProperties().size());
    assertEquals("v1", intp.getProperties().get("p1"));
    assertEquals("v1", intp.getProperty("p1"));
  }

  @Test
  public void testOverriddenProperty() {
    Properties p = new Properties();
    p.put("p1", "v1");
    Interpreter intp = new DummyInterpreter(p);
    Properties overriddenProperty = new Properties();
    overriddenProperty.put("p1", "v2");
    intp.setProperties(overriddenProperty);

    assertEquals(1, intp.getProperties().size());
    assertEquals("v2", intp.getProperties().get("p1"));
    assertEquals("v2", intp.getProperty("p1"));
  }

  @Test
  public void testPropertyWithReplacedContextFields() {
    String noteId = "testNoteId";
    String paragraphTitle = "testParagraphTitle";
    String paragraphText = "testParagraphText";
    String paragraphId = "testParagraphId";
    String user = "username";
    InterpreterContext.set(
        InterpreterContext.builder()
            .setNoteId(noteId)
            .setParagraphId(paragraphId)
            .setParagraphText(paragraphText)
            .setParagraphTitle(paragraphTitle)
            .build());

    Properties p = new Properties();
    p.put("p1", "replName #{noteId}, #{paragraphTitle}, #{paragraphId}, #{paragraphText}, " +
        "#{replName}, #{noteId}, #{user}," +
        " #{authenticationInfo}");
    Interpreter intp = new DummyInterpreter(p);
    intp.setUserName(user);
    String actual = intp.getProperty("p1");
    InterpreterContext.remove();

    assertEquals(
        String.format("replName %s, #{paragraphTitle}, #{paragraphId}, #{paragraphText}, , " +
                "%s, %s, #{authenticationInfo}", noteId,
            noteId, user),
        actual
    );
  }

  @Test
  public void testNotebookIdLogging() throws InterpreterException {

    // Add an appender that stores all of the logging output in a list we can access programmatically.
    Log4jTestAppender appender = new Log4jTestAppender();
    LOGGER.addAppender(appender);

    // Create a DummyInterpreter
    Properties p = new Properties();
    p.put("p1", "v1");
    Interpreter intp = new DummyInterpreter(p);

    // Add a notebookId to MDC
    MDC.put("notebookId","dummyNotebook");

    // Run interpreter
    intp.interpret("dummy code", InterpreterContext.builder().build());

    // There should only be one row of log information present, originating from DummyInterpreter
    Assert.assertEquals(1,appender.recordedEvents().size());

    // Assert that the log looks as it should, and that it contains the MDC information.
    // {timestamp}  INFO [main] [{{notebookId,dummyNotebook}}] org.apache.zeppelin.interpreter.InterpreterTest:{lineNumber} - test log
    LoggingEvent logLine = appender.recordedEvents().get(0);
    Assert.assertEquals(Level.INFO,logLine.getLevel());
    Assert.assertEquals("dummyNotebook",logLine.getMDC("notebookId"));
    Assert.assertEquals("com.teragrep.zep_01.interpreter.InterpreterTest$DummyInterpreter",logLine.getLocationInformation().getClassName());
    Assert.assertEquals("test log",logLine.getMessage());
    LOGGER.removeAllAppenders();
  }

  class Log4jTestAppender extends AppenderSkeleton {

    private final List<LoggingEvent> recordedEvents = new ArrayList<LoggingEvent>();
    @Override
    protected void append(LoggingEvent event) {
      recordedEvents.add(event);
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    public List<LoggingEvent> recordedEvents() {
      return new ArrayList<LoggingEvent>(recordedEvents);
    }
  }
  public static class DummyInterpreter extends Interpreter {

    public DummyInterpreter(Properties property) {
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
      LOGGER.info("test log");
      return null;
    }

    @Override
    public void cancel(InterpreterContext context) {

    }

    @Override
    public FormType getFormType() {
      return null;
    }

    @Override
    public int getProgress(InterpreterContext context) {
      return 0;
    }
  }

}
