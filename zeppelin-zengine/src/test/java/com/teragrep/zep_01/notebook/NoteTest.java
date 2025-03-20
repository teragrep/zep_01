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

package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.ui.TextBox;
import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterFactory;
import com.teragrep.zep_01.interpreter.InterpreterNotFoundException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.InterpreterSettingManager;
import com.teragrep.zep_01.interpreter.ManagedInterpreterGroup;
import com.teragrep.zep_01.notebook.repo.NotebookRepo;
import com.teragrep.zep_01.scheduler.Scheduler;
import com.teragrep.zep_01.user.AuthenticationInfo;
import com.teragrep.zep_01.user.Credentials;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NoteTest {
  @Mock
  NotebookRepo repo;

  @Mock
  ParagraphJobListener paragraphJobListener;

  @Mock
  Credentials credentials;

  @Mock
  Interpreter interpreter;

  @Mock
  ManagedInterpreterGroup interpreterGroup;

  @Mock
  InterpreterSetting interpreterSetting;

  @Mock
  Scheduler scheduler;

  List<NoteEventListener> noteEventListener = new ArrayList<>();

  @Mock
  InterpreterFactory interpreterFactory;

  @Mock
  InterpreterSettingManager interpreterSettingManager;

  private AuthenticationInfo anonymous = new AuthenticationInfo("anonymous");

  @Before
  public void setUp() {
    when(interpreter.getInterpreterGroup()).thenReturn(interpreterGroup);
    when(interpreterGroup.getInterpreterSetting()).thenReturn(interpreterSetting);
  }

  @Test
  public void runNormalTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("spark"), any())).thenReturn(interpreter);
    when(interpreter.getScheduler()).thenReturn(scheduler);

    String pText = "%spark sc.version";
    Note note = new Note("test", "test", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);

    LegacyParagraph p = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p.setText(pText);
    p.setAuthenticationInfo(anonymous);
    note.run(p.getId());

    ArgumentCaptor<LegacyParagraph> pCaptor = ArgumentCaptor.forClass(LegacyParagraph.class);
    verify(scheduler, only()).submit(pCaptor.capture());
    verify(interpreterFactory, times(1)).getInterpreter(eq("spark"), any());

    assertEquals("Paragraph text", pText, pCaptor.getValue().getText());
  }

  @Test
  public void addParagraphWithEmptyReplNameTest() {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    assertNull(p.getText());
  }

  @Test
  public void addParagraphWithLastReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("spark"), any())).thenReturn(interpreter);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p1 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%spark ");
    LegacyParagraph p2 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    assertEquals("%spark\n", p2.getText());
  }

  @Test
  public void insertParagraphWithLastReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("spark"), any())).thenReturn(interpreter);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p1 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%spark ");
    LegacyParagraph p2 = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfo.ANONYMOUS);

    assertEquals("%spark\n", p2.getText());
  }

  @Test
  public void insertParagraphWithInvalidReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("invalid"), any())).thenReturn(null);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p1 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%invalid ");
    LegacyParagraph p2 = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfo.ANONYMOUS);

    assertNull(p2.getText());
  }

  @Test
  public void insertParagraphwithUser() {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfo.ANONYMOUS);
    assertEquals("anonymous", p.getUser());
  }

  @Test
  public void clearAllParagraphOutputTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("md"), any())).thenReturn(interpreter);
    when(interpreter.getScheduler()).thenReturn(scheduler);

    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    LegacyParagraph p1 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    InterpreterResult result = new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, "result");
    p1.setResult(result);

    LegacyParagraph p2 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p2.setReturn(result, new Throwable());

    note.clearAllParagraphOutput();

    assertNull(p1.getReturn());
    assertNull(p2.getReturn());
  }


  @Test
  public void personalizedModeReturnDifferentParagraphInstancePerUser() {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    String user1 = "user1";
    String user2 = "user2";
    note.setPersonalizedMode(true);
    note.addNewParagraph(new AuthenticationInfo(user1));
    LegacyParagraph baseParagraph = note.getParagraphs().get(0);
    LegacyParagraph user1Paragraph = baseParagraph.getUserParagraph(user1);
    LegacyParagraph user2Paragraph = baseParagraph.getUserParagraph(user2);
    assertNotEquals(System.identityHashCode(baseParagraph), System.identityHashCode(user1Paragraph));
    assertNotEquals(System.identityHashCode(baseParagraph), System.identityHashCode(user2Paragraph));
    assertNotEquals(System.identityHashCode(user1Paragraph), System.identityHashCode(user2Paragraph));
  }

  public void testNoteJson() throws IOException {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    note.setName("/test_note");
    note.getConfig().put("config_1", "value_1");
    note.getInfo().put("info_1", "value_1");
    String pText = "%spark sc.version";
    LegacyParagraph p = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p.setText(pText);
    p.setResult(new InterpreterResult(InterpreterResult.Code.SUCCESS, "1.6.2"));
    p.settings.getForms().put("textbox_1", new TextBox("name", "default_name"));
    p.settings.getParams().put("textbox_1", "my_name");
    note.getAngularObjects().put("ao_1", Arrays.asList(new AngularObject("name_1", "value_1", note.getId(), p.getId(), null)));

    // test Paragraph Json
    LegacyParagraph p2 = LegacyParagraph.fromJson(p.toJson());
    assertEquals(p2.settings, p.settings);
    assertEquals(p2, p);

    // test Note Json
    Note note2 = Note.fromJson(null, note.toJson());
    assertEquals(note2, note);
  }
}
