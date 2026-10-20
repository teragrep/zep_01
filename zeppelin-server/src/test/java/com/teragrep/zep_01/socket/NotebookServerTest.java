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
package com.teragrep.zep_01.socket;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import com.teragrep.zep_01.notebook.repo.Revision;
import com.teragrep.zep_01.socket.fakes.FakeNotebookSocket;
import org.apache.thrift.TException;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectBuilder;
import com.teragrep.zep_01.interpreter.InterpreterGroup;
import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.remote.RemoteAngularObjectRegistry;
import com.teragrep.zep_01.interpreter.thrift.ParagraphInfo;
import com.teragrep.zep_01.notebook.AuthorizationService;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.Notebook;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.common.Message.OP;
import com.teragrep.zep_01.rest.AbstractTestRestApi;
import com.teragrep.zep_01.scheduler.Job;
import com.teragrep.zep_01.service.NotebookService;
import com.teragrep.zep_01.service.ServiceContext;
import com.teragrep.zep_01.user.AuthenticationInfo;
import com.teragrep.zep_01.utils.TestUtils;
import org.junit.*;
import org.junit.jupiter.api.Assertions;


/** Basic REST API tests for notebookServer. */
public class NotebookServerTest extends AbstractTestRestApi {
  private static Notebook notebook;
  private static NotebookServer notebookServer;
  private static NotebookService notebookService;
  private static AuthorizationService authorizationService;
  private HttpServletRequest mockRequest;
  private AuthenticationInfo anonymous;

  @BeforeClass
  public static void init() throws Exception {
    AbstractTestRestApi.startUp(NotebookServerTest.class.getSimpleName());
    notebook = TestUtils.getInstance(Notebook.class);
    authorizationService =  TestUtils.getInstance(AuthorizationService.class);
    notebookServer = TestUtils.getInstance(NotebookServer.class);
    notebookService = TestUtils.getInstance(NotebookService.class);
  }

  @AfterClass
  public static void destroy() throws Exception {
    AbstractTestRestApi.shutDown();
  }

  @After
  public void cleanupNotebooks() throws IOException {
    for(Note note : notebook.getAllNotes()){
      notebook.removeNote(note,AuthenticationInfo.ANONYMOUS);
    }
  }

  @Before
  public void setUp() {
    mockRequest = mock(HttpServletRequest.class);
    anonymous = AuthenticationInfo.ANONYMOUS;
  }

  @Test
  public void checkOrigin() throws UnknownHostException {
    String origin = "http://" + InetAddress.getLocalHost().getHostName() + ":8080";
    assertTrue("Origin " + origin + " is not allowed. Please check your hostname.",
          notebookServer.checkOrigin(mockRequest, origin));
  }

  @Test
  public void checkInvalidOrigin(){
    assertFalse(notebookServer.checkOrigin(mockRequest, "http://evillocalhost:8080"));
  }

  @Test
  public void testCollaborativeEditing() throws IOException {
    if (!ZeppelinConfiguration.create().isZeppelinNotebookCollaborativeModeEnable()) {
      return;
    }
    NotebookSocket sock1 = createWebSocket();
    NotebookSocket sock2 = createWebSocket();

    String noteName = "Note with millis " + System.currentTimeMillis();
    notebookServer.onMessage(sock1, new Message(OP.NEW_NOTE).put("name", noteName).toJson());
    Note createdNote = null;
    for (Note note : notebook.getAllNotes()) {
      if (note.getName().equals(noteName)) {
        createdNote = note;
        break;
      }
    }

    Message message = new Message(OP.GET_NOTE).put("id", createdNote.getId());
    notebookServer.onMessage(sock1, message.toJson());
    notebookServer.onMessage(sock2, message.toJson());

    Paragraph paragraph = createdNote.getParagraphs().get(0);
    String paragraphId = paragraph.getId();

    String[] patches = new String[]{
        "@@ -0,0 +1,3 @@\n+ABC\n",            // ABC
        "@@ -1,3 +1,4 @@\n ABC\n+%0A\n",      // press Enter
        "@@ -1,4 +1,7 @@\n ABC%0A\n+abc\n",   // abc
        "@@ -1,7 +1,45 @@\n ABC\n-%0Aabc\n+ ssss%0Aabc ssss\n" // add text in two string
    };

    int sock1SendCount = 0;
    int sock2SendCount = 0;
    reset(sock1);
    reset(sock2);
    patchParagraph(sock1, paragraphId, patches[0]);
    assertEquals("ABC", paragraph.getText());
    verify(sock1, times(sock1SendCount)).send(anyString());
    verify(sock2, times(++sock2SendCount)).send(anyString());

    patchParagraph(sock2, paragraphId, patches[1]);
    assertEquals("ABC\n", paragraph.getText());
    verify(sock1, times(++sock1SendCount)).send(anyString());
    verify(sock2, times(sock2SendCount)).send(anyString());

    patchParagraph(sock1, paragraphId, patches[2]);
    assertEquals("ABC\nabc", paragraph.getText());
    verify(sock1, times(sock1SendCount)).send(anyString());
    verify(sock2, times(++sock2SendCount)).send(anyString());

    patchParagraph(sock2, paragraphId, patches[3]);
    assertEquals("ABC ssss\nabc ssss", paragraph.getText());
    verify(sock1, times(++sock1SendCount)).send(anyString());
    verify(sock2, times(sock2SendCount)).send(anyString());
  }

  private void patchParagraph(NotebookSocket noteSocket, String paragraphId, String patch) {
    Message message = new Message(OP.PATCH_PARAGRAPH);
    message.put("patch", patch);
    message.put("id", paragraphId);
    notebookServer.onMessage(noteSocket, message.toJson());
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testMakeSureNoAngularObjectBroadcastToWebsocketWhoFireTheEvent()
          throws IOException, InterruptedException {
    Note note1 = notebook.createNote("note1", anonymous);

    // get reference to interpreterGroup
    InterpreterGroup interpreterGroup = null;
    List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
    for (InterpreterSetting setting : settings) {
      if (setting.getName().equals("md")) {
        interpreterGroup = setting.getOrCreateInterpreterGroup("anonymous", note1.getId());
        break;
      }
    }

    // start interpreter process
    Paragraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%md start remote interpreter process");
    p1.setAuthenticationInfo(anonymous);
    note1.run(p1.getId());

    // wait for paragraph finished
    while (true) {
      if (p1.getStatus() == Job.Status.FINISHED) {
        break;
      }
      Thread.sleep(100);
    }
    // sleep for 1 second to make sure job running thread finish to fire event. See ZEPPELIN-3277
    Thread.sleep(1000);

    // add angularObject
    interpreterGroup.getAngularObjectRegistry().add("object1", "value1", note1.getId(), null);

    // create two sockets and open it
    NotebookSocket sock1 = createWebSocket();
    NotebookSocket sock2 = createWebSocket();

    assertEquals(sock1, sock1);
    assertNotEquals(sock1, sock2);

    notebookServer.onOpen(sock1);
    notebookServer.onOpen(sock2);
    verify(sock1, times(0)).send(anyString()); // getNote, getAngularObject
    // open the same notebook from sockets
    notebookServer.onMessage(sock1, new Message(OP.GET_NOTE).put("id", note1.getId()).toJson());
    notebookServer.onMessage(sock2, new Message(OP.GET_NOTE).put("id", note1.getId()).toJson());

    reset(sock1);
    reset(sock2);

    // update object from sock1
    notebookServer.onMessage(sock1,
            new Message(OP.ANGULAR_OBJECT_UPDATED)
                    .put("noteId", note1.getId())
                    .put("name", "object1")
                    .put("value", "value1")
                    .put("interpreterGroupId", interpreterGroup.getId()).toJson());


    // expect object is broadcasted except for where the update is created
    verify(sock1, times(0)).send(anyString());
    verify(sock2, times(1)).send(anyString());
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testAngularObjectSaveToNote()
      throws IOException, InterruptedException {
    // create a notebook
    Note note1 = notebook.createNote("note1", "angular", anonymous);

    // get reference to interpreterGroup
    InterpreterGroup interpreterGroup = null;
    List<InterpreterSetting> settings = note1.getBindedInterpreterSettings(new ArrayList<>());
    for (InterpreterSetting setting : settings) {
      if (setting.getName().equals("angular")) {
        interpreterGroup = setting.getOrCreateInterpreterGroup("anonymous", note1.getId());
        break;
      }
    }

    // start interpreter process
    Paragraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%angular <h2>Bind here : {{COMMAND_TYPE}}</h2>");
    p1.setAuthenticationInfo(anonymous);
    note1.run(p1.getId());

    // wait for paragraph finished
    while (true) {
      if (p1.getStatus() == Job.Status.FINISHED) {
        break;
      }
      Thread.sleep(100);
    }
    // sleep for 1 second to make sure job running thread finish to fire event. See ZEPPELIN-3277
    Thread.sleep(1000);

    // create two sockets and open it
    NotebookSocket sock1 = createWebSocket();

    notebookServer.onOpen(sock1);
    verify(sock1, times(0)).send(anyString()); // getNote, getAngularObject
    // open the same notebook from sockets
    notebookServer.onMessage(sock1, new Message(OP.GET_NOTE).put("id", note1.getId()).toJson());

    reset(sock1);

    // bind object from sock1
    notebookServer.onMessage(sock1,
            new Message(OP.ANGULAR_OBJECT_CLIENT_BIND)
                    .put("noteId", note1.getId())
                    .put("paragraphId", p1.getId())
                    .put("name", "COMMAND_TYPE")
                    .put("value", "COMMAND_TYPE_VALUE")
                    .put("interpreterGroupId", interpreterGroup.getId()).toJson());
    List<AngularObject> list = note1.getAngularObjects("angular-shared_process");
    assertEquals(1, list.size());
    assertEquals(list.get(0).getNoteId(), note1.getId());
    assertEquals(list.get(0).getParagraphId(), p1.getId());
    assertEquals("COMMAND_TYPE", list.get(0).getName());
    assertEquals("COMMAND_TYPE_VALUE", list.get(0).get());
    // Check if the interpreterGroup AngularObjectRegistry is updated
    Map<String, Map<String, AngularObject>> mapRegistry = interpreterGroup.getAngularObjectRegistry().getRegistry();
    AngularObject ao = mapRegistry.get(note1.getId() + "_" + p1.getId()).get("COMMAND_TYPE");
    assertEquals("COMMAND_TYPE", ao.getName());
    assertEquals("COMMAND_TYPE_VALUE", ao.get());

    // update bind object from sock1
    notebookServer.onMessage(sock1,
            new Message(OP.ANGULAR_OBJECT_UPDATED)
                    .put("noteId", note1.getId())
                    .put("paragraphId", p1.getId())
                    .put("name", "COMMAND_TYPE")
                    .put("value", "COMMAND_TYPE_VALUE_UPDATE")
                    .put("interpreterGroupId", interpreterGroup.getId()).toJson());
    list = note1.getAngularObjects("angular-shared_process");
    assertEquals(1, list.size());
    assertEquals(list.get(0).getNoteId(), note1.getId());
    assertEquals(list.get(0).getParagraphId(), p1.getId());
    assertEquals("COMMAND_TYPE", list.get(0).getName());
    assertEquals("COMMAND_TYPE_VALUE_UPDATE", list.get(0).get());
    // Check if the interpreterGroup AngularObjectRegistry is updated
    mapRegistry = interpreterGroup.getAngularObjectRegistry().getRegistry();
    AngularObject ao1 = mapRegistry.get(note1.getId() + "_" + p1.getId()).get("COMMAND_TYPE");
    assertEquals("COMMAND_TYPE", ao1.getName());
    assertEquals("COMMAND_TYPE_VALUE_UPDATE", ao1.get());

    // unbind object from sock1
    notebookServer.onMessage(sock1,
            new Message(OP.ANGULAR_OBJECT_CLIENT_UNBIND)
                    .put("noteId", note1.getId())
                    .put("paragraphId", p1.getId())
                    .put("name", "COMMAND_TYPE")
                    .put("value", "COMMAND_TYPE_VALUE")
                    .put("interpreterGroupId", interpreterGroup.getId()).toJson());
    list = note1.getAngularObjects("angular-shared_process");
    assertEquals(0, list.size());
    // Check if the interpreterGroup AngularObjectRegistry is delete
    mapRegistry = interpreterGroup.getAngularObjectRegistry().getRegistry();
    AngularObject ao2 = mapRegistry.get(note1.getId() + "_" + p1.getId()).get("COMMAND_TYPE");
    assertNull(ao2);
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testLoadAngularObjectFromNote() throws IOException, InterruptedException {
    // create a notebook
    Note note1 = notebook.createNote("note1", anonymous);

    // get reference to interpreterGroup
    InterpreterGroup interpreterGroup = null;
    List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
    for (InterpreterSetting setting : settings) {
      if (setting.getName().equals("angular")) {
        interpreterGroup = setting.getOrCreateInterpreterGroup("anonymous", note1.getId());
        break;
      }
    }

    // start interpreter process
    Paragraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%angular <h2>Bind here : {{COMMAND_TYPE}}</h2>");
    p1.setAuthenticationInfo(anonymous);
    note1.run(p1.getId());

    // wait for paragraph finished
    while (true) {
      if (p1.getStatus() == Job.Status.FINISHED) {
        break;
      }
      Thread.sleep(100);
    }
    // sleep for 1 second to make sure job running thread finish to fire event. See ZEPPELIN-3277
    Thread.sleep(1000);

    // set note AngularObject
    AngularObject ao = new AngularObject("COMMAND_TYPE", "COMMAND_TYPE_VALUE", note1.getId(), p1.getId(), null);
    note1.addOrUpdateAngularObject("angular-shared_process", ao);

    // create sockets and open it
    NotebookSocket sock1 = createWebSocket();
    notebookServer.onOpen(sock1);

    // Check the AngularObjectRegistry of the interpreterGroup before executing GET_NOTE
    Map<String, Map<String, AngularObject>> mapRegistry1 = interpreterGroup.getAngularObjectRegistry().getRegistry();
    assertEquals(0, mapRegistry1.size());

    // open the notebook from sockets, AngularObjectRegistry that triggers the update of the interpreterGroup
    notebookServer.onMessage(sock1, new Message(OP.GET_NOTE).put("id", note1.getId()).toJson());
    Thread.sleep(1000);

    // After executing GET_NOTE, check the AngularObjectRegistry of the interpreterGroup
    Map<String, Map<String, AngularObject>> mapRegistry2 = interpreterGroup.getAngularObjectRegistry().getRegistry();
    assertEquals(2, mapRegistry1.size());
    AngularObject ao1 = mapRegistry2.get(note1.getId() + "_" + p1.getId()).get("COMMAND_TYPE");
    assertEquals("COMMAND_TYPE", ao1.getName());
    assertEquals("COMMAND_TYPE_VALUE", ao1.get());
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testImportNotebook() throws IOException {
    String msg = "{\"op\":\"IMPORT_NOTE\",\"data\":" +
        "{\"note\":{\"paragraphs\": [{\"text\": \"Test " +
        "paragraphs import\"," + "\"progressUpdateIntervalMs\":500," +
        "\"config\":{},\"settings\":{}}]," +
        "\"name\": \"Test Zeppelin notebook import\",\"config\": " +
        "{}}}}";
    Message messageReceived = notebookServer.deserializeMessage(msg);
    ServiceContext context = new ServiceContext(AuthenticationInfo.ANONYMOUS, new HashSet<>());
    Note note = notebookServer.importNote(null, context, messageReceived);;

    assertNotEquals(null, notebook.getNote(note.getId()));
    assertEquals("Test Zeppelin notebook import", notebook.getNote(note.getId()).getName());
    assertEquals("Test paragraphs import", notebook.getNote(note.getId()).getParagraphs().get(0)
            .getText());
  }

  @Test
  public void bindAngularObjectToRemoteForParagraphs() throws Exception {
    //Given
    final String varName = "name";
    final String value = "DuyHai DOAN";
    final Message messageReceived = new Message(OP.ANGULAR_OBJECT_CLIENT_BIND)
            .put("noteId", "noteId")
            .put("name", varName)
            .put("value", value)
            .put("paragraphId", "paragraphId");

    final Notebook notebook = mock(Notebook.class);
    notebookServer.setNotebook(() -> notebook);
    notebookServer.setNotebookService(() -> notebookService);
    final Note note = mock(Note.class, RETURNS_DEEP_STUBS);

    when(notebook.getNote("noteId")).thenReturn(note);
    final Paragraph paragraph = mock(Paragraph.class, RETURNS_DEEP_STUBS);
    when(note.getParagraph("paragraphId")).thenReturn(paragraph);

    final RemoteAngularObjectRegistry mdRegistry = mock(RemoteAngularObjectRegistry.class);
    final InterpreterGroup mdGroup = new InterpreterGroup("mdGroup");
    mdGroup.setAngularObjectRegistry(mdRegistry);

    when(paragraph.getBindedInterpreter().getInterpreterGroup()).thenReturn(mdGroup);

    final AngularObject<String> ao1 = AngularObjectBuilder.build(varName, value, "noteId",
            "paragraphId");

    when(mdRegistry.addAndNotifyRemoteProcess(varName, value, "noteId", "paragraphId"))
            .thenReturn(ao1);

    NotebookSocket conn = mock(NotebookSocket.class);
    NotebookSocket otherConn = mock(NotebookSocket.class);

    final String mdMsg1 = notebookServer.serializeMessage(new Message(OP.ANGULAR_OBJECT_UPDATE)
            .put("angularObject", ao1)
            .put("interpreterGroupId", "mdGroup")
            .put("noteId", "noteId")
            .put("paragraphId", "paragraphId"));

    notebookServer.getConnectionManager().noteSocketMap.put("noteId", asList(conn, otherConn));

    // When
    notebookServer.angularObjectClientBind(conn, messageReceived);

    // Then
    verify(mdRegistry, never()).addAndNotifyRemoteProcess(varName, value, "noteId", null);

    verify(otherConn).send(mdMsg1);
  }

  @Test
  public void unbindAngularObjectFromRemoteForParagraphs() throws Exception {
    //Given
    final String varName = "name";
    final String value = "val";
    final Message messageReceived = new Message(OP.ANGULAR_OBJECT_CLIENT_UNBIND)
            .put("noteId", "noteId")
            .put("name", varName)
            .put("paragraphId", "paragraphId");

    final Notebook notebook = mock(Notebook.class);
    notebookServer.setNotebook(() -> notebook);
    notebookServer.setNotebookService(() -> notebookService);
    final Note note = mock(Note.class, RETURNS_DEEP_STUBS);
    when(notebook.getNote("noteId")).thenReturn(note);
    final Paragraph paragraph = mock(Paragraph.class, RETURNS_DEEP_STUBS);
    when(note.getParagraph("paragraphId")).thenReturn(paragraph);

    final RemoteAngularObjectRegistry mdRegistry = mock(RemoteAngularObjectRegistry.class);
    final InterpreterGroup mdGroup = new InterpreterGroup("mdGroup");
    mdGroup.setAngularObjectRegistry(mdRegistry);

    when(paragraph.getBindedInterpreter().getInterpreterGroup()).thenReturn(mdGroup);

    final AngularObject<String> ao1 = AngularObjectBuilder.build(varName, value, "noteId",
            "paragraphId");
    when(mdRegistry.removeAndNotifyRemoteProcess(varName, "noteId", "paragraphId")).thenReturn(ao1);
    NotebookSocket conn = mock(NotebookSocket.class);
    NotebookSocket otherConn = mock(NotebookSocket.class);

    final String mdMsg1 = notebookServer.serializeMessage(new Message(OP.ANGULAR_OBJECT_REMOVE)
            .put("angularObject", ao1)
            .put("interpreterGroupId", "mdGroup")
            .put("noteId", "noteId")
            .put("paragraphId", "paragraphId"));

    notebookServer.getConnectionManager().noteSocketMap.put("noteId", asList(conn, otherConn));

    // When
    notebookServer.angularObjectClientUnbind(conn, messageReceived);

    // Then
    verify(mdRegistry, never()).removeAndNotifyRemoteProcess(varName, "noteId", null);

    verify(otherConn).send(mdMsg1);
  }

  @Test
  public void testCreateNoteWithDefaultInterpreterId() throws IOException {
    // create two sockets and open it
    NotebookSocket sock1 = createWebSocket();
    NotebookSocket sock2 = createWebSocket();

    assertEquals(sock1, sock1);
    assertNotEquals(sock1, sock2);

    notebookServer.onOpen(sock1);
    notebookServer.onOpen(sock2);

    String noteName = "Note with millis " + System.currentTimeMillis();
    String defaultInterpreterId = "";
    List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
    if (settings.size() > 1) {
      defaultInterpreterId = settings.get(0).getId();
    }
    // create note from sock1
    notebookServer.onMessage(sock1,
        new Message(OP.NEW_NOTE)
        .put("name", noteName)
        .put("defaultInterpreterId", defaultInterpreterId).toJson());

    int sendCount = 2;
    if (ZeppelinConfiguration.create().isZeppelinNotebookCollaborativeModeEnable()) {
      sendCount++;
    }
    // expect the events are broadcasted properly
    verify(sock1, times(sendCount)).send(anyString());

    Note createdNote = null;
    for (Note note : notebook.getAllNotes()) {
      if (note.getName().equals(noteName)) {
        createdNote = note;
        break;
      }
    }

    if (settings.size() > 1) {
      assertEquals(notebook.getInterpreterSettingManager().getDefaultInterpreterSetting(
              createdNote.getId()).getId(), defaultInterpreterId);
    }
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testRuntimeInfos() throws IOException {
    // mock note
    String msg = "{\"op\":\"IMPORT_NOTE\",\"data\":" +
        "{\"note\":{\"paragraphs\": [{\"text\": \"Test " +
        "paragraphs import\"," + "\"progressUpdateIntervalMs\":500," +
        "\"config\":{},\"settings\":{}}]," +
        "\"name\": \"Test RuntimeInfos\",\"config\": " +
        "{}}}}";
    Message messageReceived = notebookServer.deserializeMessage(msg);
    Note note = null;
    ServiceContext context = new ServiceContext(AuthenticationInfo.ANONYMOUS, new HashSet<>());
    try {
      note = notebookServer.importNote(null, context, messageReceived);
    } catch (NullPointerException | IOException e) {
      fail("Failure happened: " + e.getMessage());
    }

      assertNotEquals(null, notebook.getNote(note.getId()));
    assertNotEquals(null, note.getParagraph(0));

    String nodeId = note.getId();
    String paragraphId = note.getParagraph(0).getId();

    // update RuntimeInfos
    Map<String, String> infos = new java.util.HashMap<>();
    infos.put("jobUrl", "jobUrl_value");
    infos.put("jobLabel", "jobLabel_value");
    infos.put("label", "SPARK JOB");
    infos.put("tooltip", "View in Spark web UI");
    infos.put("noteId", nodeId);
    infos.put("paraId", paragraphId);

    notebookServer.onParaInfosReceived(nodeId, paragraphId, "spark", infos);
    Paragraph paragraph = note.getParagraph(paragraphId);

    // check RuntimeInfos
    assertTrue(paragraph.getRuntimeInfos().containsKey("jobUrl"));
    List<Map<String,String>> list = paragraph.getRuntimeInfos().get("jobUrl").getValue();
    assertEquals(1, list.size());
    Map<String, String> map = list.get(0);
    assertEquals(2, map.size());
    assertEquals("jobUrl_value", map.get("jobUrl"));
    assertEquals("jobLabel_value", map.get("jobLabel"));
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testGetParagraphList() throws IOException {
    Note note = notebook.createNote("note1", anonymous);
    Paragraph p1 = note.addNewParagraph(anonymous);
    p1.setText("%md start remote interpreter process");
    p1.setAuthenticationInfo(anonymous);
    notebook.saveNote(note, anonymous);

    String noteId = note.getId();
    String user1Id = "user1", user2Id = "user2";

    // test user1 can get anonymous's note
    List<ParagraphInfo> paragraphList0 = null;
    try {
      paragraphList0 = notebookServer.getParagraphList(user1Id, noteId);
    } catch (TException e) {
      fail("Failure happened: " + e.getMessage());
    }
    assertNotNull(user1Id + " can get anonymous's note", paragraphList0);

    // test user1 cannot get user2's note
    authorizationService.setOwners(noteId, new HashSet<>(Arrays.asList(user2Id)));
    authorizationService.setReaders(noteId, new HashSet<>(Arrays.asList(user2Id)));
    authorizationService.setRunners(noteId, new HashSet<>(Arrays.asList(user2Id)));
    authorizationService.setWriters(noteId, new HashSet<>(Arrays.asList(user2Id)));
    List<ParagraphInfo> paragraphList1 = null;
    try {
      paragraphList1 = notebookServer.getParagraphList(user1Id, noteId);
    } catch (TException e) {
      fail("Failure happened: " + e.getMessage());
    }
    assertNull(user1Id + " cannot get " + user2Id + "'s note", paragraphList1);

    // test user1 can get user2's shared note
    authorizationService.setOwners(noteId, new HashSet<>(Arrays.asList(user2Id)));
    authorizationService.setReaders(noteId, new HashSet<>(Arrays.asList(user1Id, user2Id)));
    authorizationService.setRunners(noteId, new HashSet<>(Arrays.asList(user2Id)));
    authorizationService.setWriters(noteId, new HashSet<>(Arrays.asList(user2Id)));
    List<ParagraphInfo> paragraphList2 = null;
    try {
      paragraphList2 = notebookServer.getParagraphList(user1Id, noteId);
    } catch (TException e) {
      fail("Failure happened: " + e.getMessage());
    }
    assertNotNull(user1Id + " can get " + user2Id + "'s shared note", paragraphList2);
  }

  @Ignore(value="[ERROR] Crashed tests:\n" +
          "[ERROR] com.teragrep.zep_01.socket.NotebookServerTest\n" +
          "[ERROR] ExecutionException The forked VM terminated without properly saying goodbye. VM crash or System.exit called?\n")
  @Test
  public void testNoteRevision() throws IOException {
    Note note = notebook.createNote("note1", anonymous);
    assertEquals(0, note.getParagraphCount());
    Revision firstRevision = notebook.checkpointNote(note.getId(), note.getPath(), "first commit", AuthenticationInfo.ANONYMOUS);
    List<Revision> revisionList = notebook.listRevisionHistory(note.getId(), note.getPath(), AuthenticationInfo.ANONYMOUS);
    assertEquals(1, revisionList.size());
    assertEquals(firstRevision.id(), revisionList.get(0).id());
    assertEquals("first commit", revisionList.get(0).message());

    // add one new paragraph and commit it
    note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    notebook.saveNote(note, AuthenticationInfo.ANONYMOUS);
    assertEquals(1, note.getParagraphCount());
    Revision secondRevision = notebook.checkpointNote(note.getId(), note.getPath(), "second commit", AuthenticationInfo.ANONYMOUS);

    revisionList = notebook.listRevisionHistory(note.getId(), note.getPath(), AuthenticationInfo.ANONYMOUS);
    assertEquals(2, revisionList.size());
    assertEquals(secondRevision.id(), revisionList.get(0).id());
    assertEquals("second commit", revisionList.get(0).message());
    assertEquals(firstRevision.id(), revisionList.get(1).id());
    assertEquals("first commit", revisionList.get(1).message());

    // checkout the first commit
    note = notebook.getNoteByRevision(note.getId(), note.getPath(), firstRevision.id(), AuthenticationInfo.ANONYMOUS);
    assertEquals(0, note.getParagraphCount());
  }

  /**
   * COLLABORATIVE_MODE_STATUS messages should be sent when a user creates a notebook due to automatic connection
   */
  @Test
  public void testCollaborativeNoteCreation() {
    // Create connections
    final FakeNotebookSocket alice = new FakeNotebookSocket(notebookServer);
    final FakeNotebookSocket bob = new FakeNotebookSocket(notebookServer);
    final FakeNotebookSocket charlie = new FakeNotebookSocket(notebookServer);
    notebookServer.onOpen(alice);
    notebookServer.onOpen(bob);
    notebookServer.onOpen(charlie);

    final String noteName = "Note with millis " + System.currentTimeMillis();
    final List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
    final String defaultInterpreterId = settings.size() > 1 ? settings.get(0).getId() : "";
    // alice creates note
    notebookServer.onMessage(alice,
            new Message(OP.NEW_NOTE)
                    .put("alicesNote", noteName)
                    .put("defaultInterpreterId", defaultInterpreterId).toJson());

    // NEW_NOTE sends one status message to alice only (creating a note automatically connects user to that note)
    // alice should get collaborative mode update
    Assertions.assertTrue(alice.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(alice.sentMessages().get(0).contains("\"status\": false"));
    // alice should receive the NOTE details in a separate message
    Assertions.assertTrue(alice.sentMessages().get(1).contains("NEW_NOTE"));

    // bob and charlie should not get Collaborative mode status messages
    Assertions.assertEquals(0,bob.sentMessages().size());
    Assertions.assertEquals(0,charlie.sentMessages().size());

    final int alicesMessageCount = alice.sentMessages().stream().filter(content -> {return content.contains("COLLABORATIVE_MODE_STATUS");}).collect(Collectors.toList()).size();
    // create note from charlie
    notebookServer.onMessage(charlie,
            new Message(OP.NEW_NOTE)
                    .put("charliesNote", noteName)
                    .put("defaultInterpreterId", defaultInterpreterId).toJson());

    // alice and bob should not get any additional COLLABORATIVE_MODE_STATUS messages when a user creates an unrelated note;
    Assertions.assertEquals(alicesMessageCount,alice.sentMessages().stream().filter(msg -> msg.contains("COLLABORATIVE_MODE_STATUS")).count());
    Assertions.assertEquals(0,bob.sentMessages().size());

    // NEW_NOTE sends one status message to charlie only (creating a note automatically connects user to that note)
    // charlie should get collaborative mode update
    Assertions.assertTrue(charlie.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(charlie.sentMessages().get(0).contains("\"status\": false"));
    // charlie should receive the NOTE details in a separate message
    Assertions.assertTrue(charlie.sentMessages().get(1).contains("NEW_NOTE"));

    // Make sure the notebooks were created successfully
    Assertions.assertEquals(2, notebook.getAllNotes().size());
  }

    @Test
    public void testCollaborativeNoteJoining() {
      // Create connections
      final FakeNotebookSocket alice = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket bob = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket charlie = new FakeNotebookSocket(notebookServer);
      notebookServer.onOpen(alice);
      notebookServer.onOpen(bob);
      notebookServer.onOpen(charlie);

      // create one notebook
      final List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
      final String defaultInterpreterId = settings.size() > 1 ? settings.get(0).getId() : "";
      final Note note1 = Assertions.assertDoesNotThrow(() -> notebook.createNote("collaborativeNote1", defaultInterpreterId, AuthenticationInfo.ANONYMOUS));

      // Make sure the notebook was created successfully
      Assertions.assertEquals(1, notebook.getAllNotes().size());

      // Expect correct number of COLLABORATIVE_MODE_STATUS messages when a number of users join the same notebook.
      // Alice joins note1
      notebookServer.onMessage(alice, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());

      // Each GET_NOTE should send one collaborative status update to each connected user.
      // alice should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(0).contains("\"status\": false"));
      // alice should receive the NOTE details in a separate message
      Assertions.assertTrue(alice.sentMessages().get(1).contains("NOTE"));


      // Bob joins note1
      notebookServer.onMessage(bob, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());

      // Alice and Bob should each receive a status update informing them of each other's presence.
      // alice should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(2).contains("\"status\": true"));

      // bob should get collaborative mode update
      Assertions.assertTrue(bob.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(0).contains("\"status\": true"));
      // bob should receive the NOTE details in a separate message
      Assertions.assertTrue(bob.sentMessages().get(1).contains("NOTE"));

      // Charlie joins note1
      notebookServer.onMessage(charlie, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());

      // alice should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(3).contains("\"status\": true"));

      // bob should get collaborative mode update
      Assertions.assertTrue(bob.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(2).contains("\"status\": true"));

      // charlie should get collaborative mode update
      Assertions.assertTrue(charlie.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(charlie.sentMessages().get(0).contains("\"status\": true"));
      // charlie should receive the NOTE details in a separate message
      Assertions.assertTrue(charlie.sentMessages().get(1).contains("NOTE"));
    }

    @Test
    public void testCollaborativeNoteDisconnecting() {
      // Create connections
      final FakeNotebookSocket alice = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket bob = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket charlie = new FakeNotebookSocket(notebookServer);
      notebookServer.onOpen(alice);
      notebookServer.onOpen(bob);
      notebookServer.onOpen(charlie);

      // create one notebook
      final List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
      final String defaultInterpreterId = settings.size() > 1 ? settings.get(0).getId() : "";
      final Note note1 = Assertions.assertDoesNotThrow(() -> notebook.createNote("collaborativeNote1", defaultInterpreterId, AuthenticationInfo.ANONYMOUS));

      // Make sure the notebook was created successfully
      Assertions.assertEquals(1, notebook.getAllNotes().size());

      // Connect each user to note1
      notebookServer.onMessage(alice, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());
      // alice should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(0).contains("\"status\": false"));
      // alice should receive the NOTE details in a separate message
      Assertions.assertTrue(alice.sentMessages().get(1).contains("NOTE"));

      notebookServer.onMessage(bob, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());
      // alice and bob should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(2).contains("\"status\": true"));

      Assertions.assertTrue(bob.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(0).contains("\"status\": true"));
      // bob should receive the NOTE details in a separate message
      Assertions.assertTrue(bob.sentMessages().get(1).contains("NOTE"));

      notebookServer.onMessage(charlie, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());
      // alice, bob and charlie should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(3).contains("\"status\": true"));

      Assertions.assertTrue(bob.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(2).contains("\"status\": true"));

      Assertions.assertTrue(charlie.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(charlie.sentMessages().get(0).contains("\"status\": true"));
      // charlie should receive the NOTE details in a separate message
      Assertions.assertTrue(charlie.sentMessages().get(1).contains("NOTE"));

      // Alice disconnects
      notebookServer.onClose(alice, 1, "disconnect");
      // Bob and Charlie should be notified that Alice has left.
      Assertions.assertTrue(bob.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(3).contains("\"status\": true"));

      Assertions.assertTrue(charlie.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(charlie.sentMessages().get(2).contains("\"status\": true"));

    }

  @Test
  public void testCollaborativeNoteLeaving() {
    // Create connections
    final FakeNotebookSocket alice = new FakeNotebookSocket(notebookServer);
    final FakeNotebookSocket bob = new FakeNotebookSocket(notebookServer);
    final FakeNotebookSocket charlie = new FakeNotebookSocket(notebookServer);
    notebookServer.onOpen(alice);
    notebookServer.onOpen(bob);
    notebookServer.onOpen(charlie);

    // create one notebook
    final List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
    final String defaultInterpreterId = settings.size() > 1 ? settings.get(0).getId() : "";
    final Note note1 = Assertions.assertDoesNotThrow(() -> notebook.createNote("collaborativeNote1", defaultInterpreterId, AuthenticationInfo.ANONYMOUS));

    // Make sure the notebook was created successfully
    Assertions.assertEquals(1, notebook.getAllNotes().size());

    // Connect each user to note1
    notebookServer.onMessage(alice, new Message(OP.GET_NOTE)
            .put("id", note1.getId()).toJson());
    // alice should get collaborative mode update
    Assertions.assertTrue(alice.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(alice.sentMessages().get(0).contains("\"status\": false"));
    // alice should receive the NOTE details in a separate message
    Assertions.assertTrue(alice.sentMessages().get(1).contains("NOTE"));

    notebookServer.onMessage(bob, new Message(OP.GET_NOTE)
            .put("id", note1.getId()).toJson());
    // alice and bob should get collaborative mode update
    Assertions.assertTrue(alice.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(alice.sentMessages().get(2).contains("\"status\": true"));

    Assertions.assertTrue(bob.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(bob.sentMessages().get(0).contains("\"status\": true"));
    // bob should receive the NOTE details in a separate message
    Assertions.assertTrue(bob.sentMessages().get(1).contains("NOTE"));

    notebookServer.onMessage(charlie, new Message(OP.GET_NOTE)
            .put("id", note1.getId()).toJson());
    // alice, bob and charlie should get collaborative mode update
    Assertions.assertTrue(alice.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(alice.sentMessages().get(3).contains("\"status\": true"));

    Assertions.assertTrue(bob.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(bob.sentMessages().get(2).contains("\"status\": true"));

    Assertions.assertTrue(charlie.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(charlie.sentMessages().get(0).contains("\"status\": true"));
    // charlie should receive the NOTE details in a separate message
    Assertions.assertTrue(charlie.sentMessages().get(1).contains("NOTE"));

    // Alice returns to home page
    notebookServer.onMessage(alice, new Message(OP.LIST_NOTES)
            .toJson());
    // Bob and Charlie should be notified that Alice has left.
    Assertions.assertTrue(bob.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(bob.sentMessages().get(3).contains("\"status\": true"));

    Assertions.assertTrue(charlie.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(charlie.sentMessages().get(2).contains("\"status\": true"));

    // Charlie returns to home page
    notebookServer.onMessage(charlie, new Message(OP.LIST_NOTES)
            .toJson());

    // Bob should be notified that Charlie has left.
    Assertions.assertTrue(bob.sentMessages().get(4).contains("COLLABORATIVE_MODE_STATUS"));
    Assertions.assertTrue(bob.sentMessages().get(4).contains("\"status\": false"));

  }

    @Test
    public void testSwitchingCollaborativeNote(){

      final FakeNotebookSocket alice = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket bob = new FakeNotebookSocket(notebookServer);
      final FakeNotebookSocket charlie = new FakeNotebookSocket(notebookServer);
      notebookServer.onOpen(alice);
      notebookServer.onOpen(bob);
      notebookServer.onOpen(charlie);

      // create two notebooks
      final List<InterpreterSetting> settings = notebook.getInterpreterSettingManager().get();
      final String defaultInterpreterId = settings.size() > 1 ? settings.get(0).getId() : "";
      final Note note1 = Assertions.assertDoesNotThrow(() -> notebook.createNote("collaborativeNote1", defaultInterpreterId, AuthenticationInfo.ANONYMOUS));
      final Note note2 = Assertions.assertDoesNotThrow(() -> notebook.createNote("collaborativeNote2", defaultInterpreterId, AuthenticationInfo.ANONYMOUS));

      // Make sure the notebook was created successfully
      Assertions.assertEquals(2, notebook.getAllNotes().size());


      // Each user should receive one update as they join the notebooks. Alice should receive one extra from Bob joining
      // Connect Alice and Bob to note1
      notebookServer.onMessage(alice, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());

      // alice should get collaborative mode update
      Assertions.assertTrue(alice.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(0).contains("\"status\": false"));
      // should receive the NOTE details in a separate message
      Assertions.assertTrue(alice.sentMessages().get(1).contains("NOTE"));

      notebookServer.onMessage(bob, new Message(OP.GET_NOTE)
              .put("id", note1.getId()).toJson());

      // bob should receive collaborative mode update
      Assertions.assertTrue(bob.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(0).contains("\"status\": true"));
      // should receive the NOTE details in a separate message
      Assertions.assertTrue(bob.sentMessages().get(1).contains("NOTE"));

      // alice should get collaborative mode update too
      Assertions.assertTrue(alice.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(2).contains("\"status\": true"));


      // Connect Charlie to note2
      notebookServer.onMessage(charlie, new Message(OP.GET_NOTE)
              .put("id", note2.getId()).toJson());

      Assertions.assertTrue(charlie.sentMessages().get(0).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(charlie.sentMessages().get(0).contains("\"status\": false"));
      // should receive the NOTE details in a separate message
      Assertions.assertTrue(charlie.sentMessages().get(1).contains("NOTE"));

      // Alice switches from note1 to note2
      notebookServer.onMessage(alice, new Message(OP.GET_NOTE)
              .put("id", note2.getId()).toJson());

      // Alice and Charlie should both receive a COLLABORATIVE_STATUS_MESSAGE as they join the notebook with Charlie. Bob should receive one too as Alice left their notebook.
      Assertions.assertTrue(alice.sentMessages().get(3).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(alice.sentMessages().get(3).contains("\"status\": true"));
      // should receive the NOTE details in a separate message
      Assertions.assertTrue(alice.sentMessages().get(4).contains("NOTE"));

      Assertions.assertTrue(charlie.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(charlie.sentMessages().get(2).contains("\"status\": true"));

      Assertions.assertTrue(bob.sentMessages().get(2).contains("COLLABORATIVE_MODE_STATUS"));
      Assertions.assertTrue(bob.sentMessages().get(2).contains("\"status\": false"));

  }

  private NotebookSocket createWebSocket() {
    NotebookSocket sock = mock(NotebookSocket.class);
    when(sock.getRequest()).thenReturn(mockRequest);
    return sock;
  }
}
