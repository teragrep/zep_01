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

package org.apache.zeppelin.interpreter;

import com.teragrep.zep_04.interpreter.Code;
import com.teragrep.zep_04.interpreter.InterpreterResult;
import com.teragrep.zep_04.interpreter.Type;
import org.junit.Test;
import org.junit.Ignore;

import static org.junit.Assert.assertEquals;


public class InterpreterResultTest {

  @Test
  public void testTextType() {

    InterpreterResult result = new InterpreterResultImpl(
            Code.SUCCESS,
        "this is a TEXT type");
    assertEquals("No magic", Type.TEXT, result.message().get(0).getType());
    result = new InterpreterResultImpl(Code.SUCCESS, "%this is a TEXT type");
    assertEquals("No magic", Type.TEXT, result.message().get(0).getType());
    result = new InterpreterResultImpl(Code.SUCCESS, "%\n");
    assertEquals(0, result.message().size());
  }

  @Test
  public void testSimpleMagicType() {
    InterpreterResult result = null;

    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table col1\tcol2\naaa\t123\n");
    assertEquals(Type.TABLE, result.message().get(0).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table\ncol1\tcol2\naaa\t123\n");
    assertEquals(Type.TABLE, result.message().get(0).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before magic word\n%table col1\tcol2\naaa\t123\n");
    assertEquals(Type.TABLE, result.message().get(1).getType());
  }

  @Test
  public void testComplexMagicType() {
    InterpreterResult result = null;

    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before %table col1\tcol2\naaa\t123\n");
    assertEquals("some text before magic return magic",
        Type.TEXT, result.message().get(0).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before\n%table col1\tcol2\naaa\t123\n");
    assertEquals("some text before magic return magic",
        Type.TEXT, result.message().get(0).getType());
    assertEquals("some text before magic return magic",
        Type.TABLE, result.message().get(1).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%html  <h3> This is a hack </h3> %table\n col1\tcol2\naaa\t123\n");
    assertEquals("magic A before magic B return magic A",
        Type.HTML, result.message().get(0).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before magic word %table col1\tcol2\naaa\t123\n %html  " +
            "<h3> This is a hack </h3>");
    assertEquals("text & magic A before magic B return magic A",
        Type.TEXT, result.message().get(0).getType());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table col1\tcol2\naaa\t123\n %html  <h3> This is a hack </h3> %table col1\naaa\n123\n");
    assertEquals("magic A, magic B, magic a' return magic A",
        Type.TABLE, result.message().get(0).getType());
  }

  @Test
  public void testSimpleMagicData() {

    InterpreterResult result = null;

    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table col1\tcol2\naaa\t123\n");
    assertEquals("%table col1\tcol2\naaa\t123\n",
        "col1\tcol2\naaa\t123\n", result.message().get(0).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table\ncol1\tcol2\naaa\t123\n");
    assertEquals("%table\ncol1\tcol2\naaa\t123\n",
        "col1\tcol2\naaa\t123\n", result.message().get(0).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before magic word\n%table col1\tcol2\naaa\t123\n");
    assertEquals("some text before magic word\n%table col1\tcol2\naaa\t123\n",
        "col1\tcol2\naaa\t123\n", result.message().get(1).getData());
  }

  @Ignore(value="Was not enabled")
  @Test
  public void testComplexMagicData() {

    InterpreterResult result = null;

    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before\n%table col1\tcol2\naaa\t123\n");
    assertEquals("text before %table", "some text before\n", result.message().get(0).getData());
    assertEquals("text after %table", "col1\tcol2\naaa\t123\n", result.message().get(1).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%html  <h3> This is a hack </h3>\n%table\ncol1\tcol2\naaa\t123\n");
    assertEquals(" <h3> This is a hack </h3>\n", result.message().get(0).getData());
    assertEquals("col1\tcol2\naaa\t123\n", result.message().get(1).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "some text before magic word\n%table col1\tcol2\naaa\t123\n\n%html " +
            "<h3> This is a hack </h3>");
    assertEquals("<h3> This is a hack </h3>", result.message().get(2).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table col1\tcol2\naaa\t123\n\n%html  <h3> This is a hack </h3>\n%table col1\naaa\n123\n");
    assertEquals("col1\naaa\n123\n", result.message().get(2).getData());
    result = new InterpreterResultImpl(
            Code.SUCCESS,
        "%table " + "col1\tcol2\naaa\t123\n\n%table col1\naaa\n123\n");
    assertEquals("col1\tcol2\naaa\t123\n", result.message().get(0).getData());
    assertEquals("col1\naaa\n123\n", result.message().get(1).getData());
  }

  @Test
  public void testToString() {
    assertEquals("%html hello", new InterpreterResultImpl(
            Code.SUCCESS,
        "%html hello").toString());
  }

}
