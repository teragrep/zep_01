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


package com.teragrep.zep_01.interpreter.recovery;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.AbstractInterpreterTest;
import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOption;
import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreter;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.NoteInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class LocalRecoveryStorageTest extends AbstractInterpreterTest {
  private File recoveryDir = null;

  @Before
  public void setUp() throws Exception {
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_STORAGE_CLASS.getVarName(),
            LocalRecoveryStorage.class.getName());
    recoveryDir = new File("target/").toPath().toAbsolutePath().toFile();
    recoveryDir.mkdirs();
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_DIR.getVarName(), recoveryDir.getAbsolutePath());
    super.setUp();

    Note note1 = new Note(new NoteInfo("note1", "/note_1"));
    Note note2 = new Note(new NoteInfo("note2", "/note_2"));
    when(mockNotebook.getNote("note1")).thenReturn(note1);
    when(mockNotebook.getNote("note2")).thenReturn(note2);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_STORAGE_CLASS.getVarName());
  }

  @Test
  public void testSingleInterpreterProcess() throws InterpreterException, IOException {
    InterpreterSetting interpreterSetting = interpreterSettingManager.getByName("test");
    interpreterSetting.getOption().setPerUser(InterpreterOption.SHARED);

    Interpreter interpreter1 = interpreterSetting.getDefaultInterpreter("user1", "note1");
    RemoteInterpreter remoteInterpreter1 = (RemoteInterpreter) interpreter1;
    InterpreterContext context1 = InterpreterContext.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .build();
    remoteInterpreter1.interpret("hello", context1);

    assertEquals(1, interpreterSettingManager.getRecoveryStorage().restore().size());

    interpreterSetting.close();
    assertEquals(0, interpreterSettingManager.getRecoveryStorage().restore().size());
  }

  @Test
  public void testMultipleInterpreterProcess() throws InterpreterException, IOException {
    InterpreterSetting interpreterSetting = interpreterSettingManager.getByName("test");
    interpreterSetting.getOption().setPerUser(InterpreterOption.ISOLATED);

    Interpreter interpreter1 = interpreterSetting.getDefaultInterpreter("user1", "note1");
    RemoteInterpreter remoteInterpreter1 = (RemoteInterpreter) interpreter1;
    InterpreterContext context1 = InterpreterContext.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .build();
    remoteInterpreter1.interpret("hello", context1);
    assertEquals(1, interpreterSettingManager.getRecoveryStorage().restore().size());

    Interpreter interpreter2 = interpreterSetting.getDefaultInterpreter("user2", "note2");
    RemoteInterpreter remoteInterpreter2 = (RemoteInterpreter) interpreter2;
    InterpreterContext context2 = InterpreterContext.builder()
            .setNoteId("noteId")
            .setParagraphId("paragraphId")
            .build();
    remoteInterpreter2.interpret("hello", context2);

    assertEquals(2, interpreterSettingManager.getRecoveryStorage().restore().size());

    interpreterSettingManager.restart(interpreterSetting.getId(), "user1", "note1");
    assertEquals(1, interpreterSettingManager.getRecoveryStorage().restore().size());

    interpreterSetting.close();
    assertEquals(0, interpreterSettingManager.getRecoveryStorage().restore().size());
  }
}
