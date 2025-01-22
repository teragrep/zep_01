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

package org.apache.zeppelin.interpreter.remote;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.thrift.ParagraphInfo;
import org.apache.zeppelin.interpreter.xref.Code;
import org.apache.zeppelin.interpreter.xref.InterpreterException;
import org.apache.zeppelin.interpreter.xref.InterpreterResult;
import org.apache.zeppelin.interpreter.xref.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Test for remote interpreter output stream
 */
public class RemoteInterpreterOutputTestStream extends AbstractInterpreterTest
    implements RemoteInterpreterProcessListener {


  private InterpreterSetting interpreterSetting;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    interpreterSetting = interpreterSettingManager.get("test");
  }

  @After
  public void tearDown() throws Exception {
    interpreterSetting.close();
  }

  private InterpreterContextImpl createInterpreterContext() {
    return InterpreterContextImpl.builder()
        .setNoteId("noteId")
        .setParagraphId("id")
        .build();
  }

  @Test
  public void testInterpreterResultOnly() throws InterpreterException {
    RemoteInterpreter intp = (RemoteInterpreter) interpreterSetting.getInterpreter("user1", "note1", "mock_stream");
    InterpreterResult ret = intp.interpret("SUCCESS::staticresult", createInterpreterContext());
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals("staticresult", ret.message().get(0).getData());

    ret = intp.interpret("SUCCESS::staticresult2", createInterpreterContext());
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals("staticresult2", ret.message().get(0).getData());

    ret = intp.interpret("ERROR::staticresult3", createInterpreterContext());
    assertEquals(Code.ERROR, ret.code());
    assertEquals("staticresult3", ret.message().get(0).getData());
  }

  @Test
  public void testInterpreterOutputStreamOnly() throws InterpreterException {
    RemoteInterpreter intp = (RemoteInterpreter) interpreterSetting.getInterpreter("user1", "note1", "mock_stream");
    InterpreterResult ret = intp.interpret("SUCCESS:streamresult:", createInterpreterContext());
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals("streamresult", ret.message().get(0).getData());

    ret = intp.interpret("ERROR:streamresult2:", createInterpreterContext());
    assertEquals(Code.ERROR, ret.code());
    assertEquals("streamresult2", ret.message().get(0).getData());
  }

  @Test
  public void testInterpreterResultOutputStreamMixed() throws InterpreterException {
    RemoteInterpreter intp = (RemoteInterpreter) interpreterSetting.getInterpreter("user1", "note1", "mock_stream");
    InterpreterResult ret = intp.interpret("SUCCESS:stream:static", createInterpreterContext());
    assertEquals(Code.SUCCESS, ret.code());
    assertEquals("stream", ret.message().get(0).getData());
    assertEquals("static", ret.message().get(1).getData());
  }

  @Test
  public void testOutputType() throws InterpreterException {
    RemoteInterpreter intp = (RemoteInterpreter) interpreterSetting.getInterpreter("user1", "note1", "mock_stream");

    InterpreterResult ret = intp.interpret("SUCCESS:%html hello:", createInterpreterContext());
    assertEquals(Type.HTML, ret.message().get(0).getType());
    assertEquals("hello", ret.message().get(0).getData());

    ret = intp.interpret("SUCCESS:%html\nhello:", createInterpreterContext());
    assertEquals(Type.HTML, ret.message().get(0).getType());
    assertEquals("hello", ret.message().get(0).getData());

    ret = intp.interpret("SUCCESS:%html hello:%angular world", createInterpreterContext());
    assertEquals(Type.HTML, ret.message().get(0).getType());
    assertEquals("hello", ret.message().get(0).getData());
    assertEquals(Type.ANGULAR, ret.message().get(1).getType());
    assertEquals("world", ret.message().get(1).getData());
  }

  @Override
  public void onOutputAppend(String noteId, String paragraphId, int index, String output) {

  }

  @Override
  public void onOutputUpdated(String noteId, String paragraphId, int index, Type type, String output) {

  }

  @Override
  public void onOutputClear(String noteId, String paragraphId) {

  }

  @Override
  public void runParagraphs(String noteId, List<Integer> paragraphIndices, List<String> paragraphIds, String curParagraphId) throws IOException {

  }

  @Override
  public void onParaInfosReceived(String noteId, String paragraphId,
      String interpreterSettingId, Map<String, String> metaInfos) {
  }

  @Override
  public List<ParagraphInfo> getParagraphList(String user, String noteId) {
    return null;
  }

  @Override
  public void checkpointOutput(String noteId, String paragraphId) {

  }
}
