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

package org.apache.zeppelin.notebook;

import org.apache.zeppelin.display.AngularObjectImpl;
import org.apache.zeppelin.display.ui.TextBox;
import org.apache.zeppelin.interpreter.*;
import com.teragrep.zep_04.interpreter.Code;
import com.teragrep.zep_04.interpreter.Interpreter;
import com.teragrep.zep_04.interpreter.InterpreterResult;
import com.teragrep.zep_04.interpreter.Type;
import com.teragrep.zep_04.scheduler.Scheduler;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import com.teragrep.zep_04.user.AuthenticationInfo;
import org.apache.zeppelin.user.AuthenticationInfoImpl;
import org.apache.zeppelin.user.Credentials;
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

  private AuthenticationInfo anonymous = new AuthenticationInfoImpl("anonymous");

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

    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p.setText(pText);
    p.setAuthenticationInfo(anonymous);
    note.run(p.getId());

    ArgumentCaptor<Paragraph> pCaptor = ArgumentCaptor.forClass(Paragraph.class);
    verify(scheduler, only()).submit(pCaptor.capture());
    verify(interpreterFactory, times(1)).getInterpreter(eq("spark"), any());

    assertEquals("Paragraph text", pText, pCaptor.getValue().getText());
  }

  @Test
  public void addParagraphWithEmptyReplNameTest() {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    assertNull(p.getText());
  }

  @Test
  public void addParagraphWithLastReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("spark"), any())).thenReturn(interpreter);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p1 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p1.setText("%spark ");
    Paragraph p2 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);

    assertEquals("%spark\n", p2.getText());
  }

  @Test
  public void insertParagraphWithLastReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("spark"), any())).thenReturn(interpreter);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p1 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p1.setText("%spark ");
    Paragraph p2 = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfoImpl.ANONYMOUS);

    assertEquals("%spark\n", p2.getText());
  }

  @Test
  public void insertParagraphWithInvalidReplNameTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("invalid"), any())).thenReturn(null);
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p1 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p1.setText("%invalid ");
    Paragraph p2 = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfoImpl.ANONYMOUS);

    assertNull(p2.getText());
  }

  @Test
  public void insertParagraphwithUser() {
    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p = note.insertNewParagraph(note.getParagraphs().size(), AuthenticationInfoImpl.ANONYMOUS);
    assertEquals("anonymous", p.getUser());
  }

  @Test
  public void clearAllParagraphOutputTest() throws InterpreterNotFoundException {
    when(interpreterFactory.getInterpreter(eq("md"), any())).thenReturn(interpreter);
    when(interpreter.getScheduler()).thenReturn(scheduler);

    Note note = new Note("test", "", interpreterFactory, interpreterSettingManager, paragraphJobListener, credentials, noteEventListener);
    Paragraph p1 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    InterpreterResult result = new InterpreterResultImpl(Code.SUCCESS, Type.TEXT, "result");
    p1.setResult(result);

    Paragraph p2 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
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
    note.addNewParagraph(new AuthenticationInfoImpl(user1));
    Paragraph baseParagraph = note.getParagraphs().get(0);
    Paragraph user1Paragraph = baseParagraph.getUserParagraph(user1);
    Paragraph user2Paragraph = baseParagraph.getUserParagraph(user2);
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
    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p.setText(pText);
    p.setResult(new InterpreterResultImpl(Code.SUCCESS, "1.6.2"));
    p.settings.getForms().put("textbox_1", new TextBox("name", "default_name"));
    p.settings.getParams().put("textbox_1", "my_name");
    note.getAngularObjects().put("ao_1", Arrays.asList(new AngularObjectImpl("name_1", "value_1", note.getId(), p.getId(), null)));

    // test Paragraph Json
    Paragraph p2 = Paragraph.fromJson(p.toJson());
    assertEquals(p2.settings, p.settings);
    assertEquals(p2, p);

    // test Note Json
    Note note2 = Note.fromJson(null, note.toJson());
    assertEquals(note2, note);
  }
}
