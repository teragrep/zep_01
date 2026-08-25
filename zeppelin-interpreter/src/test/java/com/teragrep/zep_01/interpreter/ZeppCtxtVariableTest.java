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

import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.resource.LocalResourcePool;
import com.teragrep.zep_01.resource.ResourcePool;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import static org.junit.Assert.assertEquals;

public class ZeppCtxtVariableTest {

  private ResourcePool resourcePool;
  private AngularObjectRegistry registry;
  private String testNoteId = "testNote";
  private String testParagraphId = "testParagraph";

  @Before
  public void setUp() throws Exception {
    registry = new AngularObjectRegistry("testInterpreterGroup",null);

    resourcePool = new LocalResourcePool("ZeppelinContextVariableInterpolationTest");
    resourcePool.put("PI", "3.141592");
  }
  @Test
  public void testInterpolateGlobalKey(){
    InterpreterContext context = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .build();
    registry.add("PI",3.1415,null,null);

    String result = AbstractInterpreter.interpolate("Value of ${PI} is 3.1415", context);
    Assertions.assertEquals("Value of 3.1415 is 3.1415",result);
  }

  @Test
  public void testInterpolateNotebookKey(){
    InterpreterContext context = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .setNoteId(testNoteId)
            .setParagraphId(testParagraphId)
            .build();
    registry.add("PI",3.1415,testNoteId,null);
    String result = AbstractInterpreter.interpolate("Value of ${PI} is 3.1415", context);
    Assertions.assertEquals("Value of 3.1415 is 3.1415",result);
  }

  @Test
  public void testInterpolateParagraphKey(){
    InterpreterContext context = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .setNoteId(testNoteId)
            .setParagraphId(testParagraphId)
            .build();
    registry.add("PI",3.1415,testNoteId,testParagraphId);
    String result = AbstractInterpreter.interpolate("Value of ${PI} is 3.1415", context);
    Assertions.assertEquals("Value of 3.1415 is 3.1415",result);
  }

  @Test
  public void testInterpolateKeyOverrides(){
    //The most specific key should be used, selected by values in InterpreterContext, if multiple AngularObjects with the same key are present
    InterpreterContext context = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .setNoteId(testNoteId)
            .setParagraphId(testParagraphId)
            .build();

    registry.add("PI",3,null,null);
    registry.add("PI",3.14,testNoteId,null);
    registry.add("PI",3.1415,testNoteId,testParagraphId);

    String result = AbstractInterpreter.interpolate("Value of ${PI} is 3.1415", context);
    Assertions.assertEquals("Value of 3.1415 is 3.1415",result);

    InterpreterContext context2 = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .setNoteId(testNoteId)
            .setParagraphId(null)
            .build();


    String result2 = AbstractInterpreter.interpolate("Value of ${PI} is 3.14", context2);
    Assertions.assertEquals("Value of 3.14 is 3.14",result2);


    // ResourcePool should override AngularObjectRegistry
    InterpreterContext context3 = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .setNoteId(testNoteId)
            .setParagraphId(testParagraphId)
            .setResourcePool(resourcePool)
            .build();


    String result3 = AbstractInterpreter.interpolate("Value of ${PI} is 3.141592", context3);
    Assertions.assertEquals("Value of 3.141592 is 3.141592",result3);
  }


  @Test
  public void testInterpolateMissingKey(){
    InterpreterContext context = InterpreterContext.builder()
            .setAngularObjectRegistry(registry)
            .build();
    Assertions.assertThrows(IllegalArgumentException.class, ()->{AbstractInterpreter.interpolate("Value of ${PI} is 3.14", context);});
  }

}
