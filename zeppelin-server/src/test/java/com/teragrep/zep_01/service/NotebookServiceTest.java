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


package com.teragrep.zep_01.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.interpreter.*;
import com.teragrep.zep_01.socket.NotebookServer;
import org.apache.commons.lang3.StringUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.Interpreter.FormType;
import com.teragrep.zep_01.interpreter.InterpreterResult.Code;
import com.teragrep.zep_01.notebook.AuthorizationService;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.NoteInfo;
import com.teragrep.zep_01.notebook.NoteManager;
import com.teragrep.zep_01.notebook.Notebook;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.notebook.exception.NotePathAlreadyExistsException;
import com.teragrep.zep_01.notebook.repo.NotebookRepo;
import com.teragrep.zep_01.notebook.repo.VFSNotebookRepo;
import com.teragrep.zep_01.notebook.scheduler.QuartzSchedulerService;
import com.teragrep.zep_01.search.LuceneSearch;
import com.teragrep.zep_01.search.SearchService;
import com.teragrep.zep_01.user.AuthenticationInfo;
import com.teragrep.zep_01.user.Credentials;
import org.junit.*;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;

import com.google.gson.Gson;

// This test has to use mocking extensively due to severe coupling between Zeppelin's components making it almost impossible to instantiate them all without making the test unreadable and slow
public class NotebookServiceTest {

  private static NotebookService notebookService;
  private AngularObjectRegistry anonymousAngularObjectRegistry;
  private AngularObjectRegistry user1AngularObjectRegistry;
  private File notebookDir;
  private SearchService searchService;
  private ServiceContext context =
      new ServiceContext(AuthenticationInfo.ANONYMOUS, new HashSet<>());

  private ServiceCallback callback = mock(ServiceCallback.class);

  private Gson gson = new Gson();


  @Before
  public void setUp() throws Exception {
    notebookDir = new File("target/notebookDir").toPath().toAbsolutePath().toFile();
    notebookDir.mkdirs();
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_SEARCH_INDEX_PATH.getVarName(), notebookDir.getAbsolutePath() + "/lucene");
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(),
            notebookDir.getAbsolutePath());
    ZeppelinConfiguration zeppelinConfiguration = ZeppelinConfiguration.create();
    NotebookRepo notebookRepo = new VFSNotebookRepo();
    notebookRepo.init(zeppelinConfiguration);

    InterpreterSettingManager mockInterpreterSettingManager = mock(InterpreterSettingManager.class);
    InterpreterFactory mockInterpreterFactory = mock(InterpreterFactory.class);
    Interpreter mockInterpreter = mock(Interpreter.class);
    when(mockInterpreterFactory.getInterpreter(any(), any()))
        .thenReturn(mockInterpreter);
    when(mockInterpreter.interpret(eq("invalid_code"), any()))
        .thenReturn(new InterpreterResult(Code.ERROR, "failed"));
    when(mockInterpreter.interpret(eq("1+1"), any()))
        .thenReturn(new InterpreterResult(Code.SUCCESS, "succeed"));
    doCallRealMethod().when(mockInterpreter).getScheduler();
    when(mockInterpreter.getFormType()).thenReturn(FormType.NATIVE);
    NotebookServer mockNotebookServer = mock(NotebookServer.class);

    ManagedInterpreterGroup mockInterpreterGroupUser1 = mock(ManagedInterpreterGroup.class);
    when(mockInterpreterGroupUser1.getId()).thenReturn("test-user1");
    user1AngularObjectRegistry = new AngularObjectRegistry(mockInterpreterGroupUser1.getId(),mockNotebookServer);
    when(mockInterpreterGroupUser1.getAngularObjectRegistry()).thenReturn(user1AngularObjectRegistry);

    ManagedInterpreterGroup mockInterpreterGroupAnonymous = mock(ManagedInterpreterGroup.class);
    when(mockInterpreterGroupAnonymous.getId()).thenReturn("test-anonymous");
    anonymousAngularObjectRegistry = new AngularObjectRegistry(mockInterpreterGroupAnonymous.getId(),mockNotebookServer);
    when(mockInterpreterGroupAnonymous.getAngularObjectRegistry()).thenReturn(anonymousAngularObjectRegistry);



    when(mockInterpreter.getInterpreterGroup()).thenReturn(mockInterpreterGroupAnonymous);
    InterpreterSetting mockInterpreterSetting = mock(InterpreterSetting.class);
    when(mockInterpreterSetting.getInterpreterGroup(eq("anonymous"),any())).thenReturn(mockInterpreterGroupAnonymous);
    when(mockInterpreterSetting.getInterpreterGroup(eq("user1"),any())).thenReturn(mockInterpreterGroupUser1);

    when(mockInterpreterSetting.isUserAuthorized(any())).thenReturn(true);
    when(mockInterpreterGroupAnonymous.getInterpreterSetting()).thenReturn(mockInterpreterSetting);
    when(mockInterpreterSetting.getStatus()).thenReturn(InterpreterSetting.Status.READY);
    searchService = new LuceneSearch(zeppelinConfiguration);
    Credentials credentials = new Credentials();
    NoteManager noteManager = new NoteManager(notebookRepo);
    AuthorizationService authorizationService = new AuthorizationService(noteManager, zeppelinConfiguration);
    Notebook notebook =
        new Notebook(
            zeppelinConfiguration,
            authorizationService,
            notebookRepo,
            noteManager,
            mockInterpreterFactory,
            mockInterpreterSettingManager,
            searchService,
            credentials,
            null);

    QuartzSchedulerService schedulerService = new QuartzSchedulerService(zeppelinConfiguration, notebook);
    schedulerService.waitForFinishInit();
    notebookService =
        new NotebookService(
            notebook, authorizationService, zeppelinConfiguration, schedulerService);

    String interpreterName = "test";
    when(mockInterpreterSetting.getName()).thenReturn(interpreterName);
    when(mockInterpreterSettingManager.getDefaultInterpreterSetting())
        .thenReturn(mockInterpreterSetting);
  }

  @After
  public void tearDown() {
    searchService.close();
  }
  // Looking for an AngularObject from a registry where it exists should result in callback being called with the specific AngularObject
  @Test
  public void testUpdateParagraphResultWithExistingAngularObject() {
    // Create a note as anonymous user
    Note note1 = Assertions.assertDoesNotThrow(() -> notebookService.createNote("/folder_1/note1", "test", true, context, callback));
    String noteId = note1.getId();
    String paragraphId = note1.getParagraphs().get(0).getId();
    Assertions.assertEquals("note1",note1.getName());
    Assertions.assertEquals(1,note1.getParagraphCount());
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(note1, context));
    reset(callback);

    // Create an AJAXRequestAngularObject to the registry of anonymous user.
    String paragraphOwner = note1.getParagraphs().get(0).getUser();
    AngularObject ajaxRequest = anonymousAngularObjectRegistry.add("AJAXRequest_"+paragraphId,"{}",noteId,paragraphId);

    // Send a updateParagraphResult with paragraph's owners username, and verify that the ServiceCallback receives the AngularObject after the message is processed.
    ServiceContext editedContext = new ServiceContext(new AuthenticationInfo(paragraphOwner),new HashSet<>());
    Assertions.assertDoesNotThrow(() -> notebookService.updateParagraphResult(noteId,paragraphId,"test-"+paragraphOwner,0,0,0,"",editedContext,callback));
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(ajaxRequest, editedContext));

    // Not deleting the notebook file after running the test will cause other tests to fail
    Path notebookPath = Paths.get(notebookDir.getPath(),note1.getPath()+"_"+noteId+".zpln");
    Assertions.assertDoesNotThrow(()->{Files.delete(notebookPath);});
  }

  // Looking for an AngularObject from a registry where it doesn't exist should result in callback being called with 'null'
  @Test
  public void testUpdateParagraphResultWithNoAngularObject()  {
    // Create a note as anonymous user
    Note note1 = Assertions.assertDoesNotThrow(() -> notebookService.createNote("/folder_1/note1", "test", true, context, callback));
    String noteId = note1.getId();
    String paragraphId = note1.getParagraphs().get(0).getId();
    Assertions.assertEquals("note1",note1.getName());
    Assertions.assertEquals(1,note1.getParagraphCount());
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(note1, context));
    reset(callback);

    // Do not add AJAXRequestAngularObject to the registry.
    String paragraphOwner = note1.getParagraphs().get(0).getUser();

    // Send a updateParagraphResult message from the same user, and verify that the ServiceCallback receives a null from the AngularObjectRegistry, as the expected Object does not exist.
    ServiceContext editedContext = new ServiceContext(new AuthenticationInfo(paragraphOwner),new HashSet<>());
    Assertions.assertDoesNotThrow(() -> notebookService.updateParagraphResult(noteId,paragraphId,"test-"+paragraphOwner,0,0,0,"",editedContext,callback));
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(null, editedContext));

    // Not deleting the notebook file after running the test will cause other tests to fail
    Path notebookPath = Paths.get(notebookDir.getPath(),note1.getPath()+"_"+noteId+".zpln");
    Assertions.assertDoesNotThrow(()->{Files.delete(notebookPath);});
  }

  // Looking for an AngularObject from another user's registry is possible by overriding the user value of the ServiceContext. If the object exists, should result in callback being called with the specific AngularObject
  @Test
  public void testUpdateParagraphResultWithOverriddenServiceContext() {
    // Create a note as anonymous user
    Note note1 = Assertions.assertDoesNotThrow(() -> notebookService.createNote("/folder_1/note1", "test", true, context, callback));
    String noteId = note1.getId();
    String paragraphId = note1.getParagraphs().get(0).getId();
    Assertions.assertEquals("note1",note1.getName());
    Assertions.assertEquals(1,note1.getParagraphCount());
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(note1, context));
    reset(callback);
    // Add the AngularObject to user1's registry
    AngularObject ajaxRequest = user1AngularObjectRegistry.add("AJAXRequest_"+paragraphId,"{}",noteId,paragraphId);

    // Change the owner of the paragraph to another user
    note1.getParagraphs().get(0).setAuthenticationInfo(new AuthenticationInfo("user1"));
    String paragraphOwner = note1.getParagraphs().get(0).getUser();

    // Send a updateParagraphResult message from the paragraph's new owner and verify that the ServiceCallback receives the AngularObject from the registry.
    ServiceContext editedContext = new ServiceContext(new AuthenticationInfo(paragraphOwner),new HashSet<>());
    Assertions.assertDoesNotThrow(() -> notebookService.updateParagraphResult(noteId,paragraphId,"test-"+paragraphOwner,0,0,0,"",editedContext,callback));
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(ajaxRequest, editedContext));

    // Not deleting the notebook file after running the test will cause other tests to fail
    Path notebookPath = Paths.get(notebookDir.getPath(),note1.getPath()+"_"+noteId+".zpln");
    Assertions.assertDoesNotThrow(()->{Files.delete(notebookPath);});
  }

  // Looking for an AngularObject from another user's registry is possible, but if it doesn't exist, should result in callback being called with 'null'
  @Test
  public void testUpdateParagraphResultFromAnotherUsersRegistryWithNoAngularObject() {
    // Create a note as anonymous user
    Note note1 =     Assertions.assertDoesNotThrow(() -> notebookService.createNote("/folder_1/note1", "test", true, context, callback));
    String noteId = note1.getId();
    String paragraphId = note1.getParagraphs().get(0).getId();
    Assertions.assertEquals("note1",note1.getName());
    Assertions.assertEquals(1,note1.getParagraphCount());
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(note1, context));
    reset(callback);

    // Change the owner of the paragraph to a user who doesn't have an AngularObjectRegistry active
    note1.getParagraphs().get(0).setAuthenticationInfo(new AuthenticationInfo("user1"));
    String paragraphOwner = note1.getParagraphs().get(0).getUser();

    // Send a updateParagraphResult message from the paragraph's new owner and verify that the ServiceCallback receives a null from the AngularObjectRegistry, as the expected AngularObjectRegistry does not exist.
    ServiceContext editedContext = new ServiceContext(new AuthenticationInfo(paragraphOwner),new HashSet<>());
    Assertions.assertDoesNotThrow(() -> notebookService.updateParagraphResult(noteId,paragraphId,"test-"+paragraphOwner,0,0,0,"",editedContext,callback));
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(null, editedContext));

    // Not deleting the notebook file after running the test will cause other tests to fail
    Path notebookPath = Paths.get(notebookDir.getPath(),note1.getPath()+"_"+noteId+".zpln");
    Assertions.assertDoesNotThrow(()->{Files.delete(notebookPath);});
  }

  @Test
  public void testUpdateParagraphResultFromUserWithNoInterpreterGroup() {
    // Create a note as anonymous user
    Note note1 = Assertions.assertDoesNotThrow(() -> notebookService.createNote("/folder_1/note1", "test", true, context, callback));
    String noteId = note1.getId();
    Assertions.assertEquals("note1",note1.getName());
    Assertions.assertEquals(1,note1.getParagraphCount());
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(note1, context));
    reset(callback);
    String paragraphId = note1.getParagraphs().get(0).getId();

    // Change the owner of the paragraph to a user who doesn't have an InterpreterGroup
    note1.getParagraphs().get(0).setAuthenticationInfo(new AuthenticationInfo("user2"));
    String paragraphOwner = note1.getParagraphs().get(0).getUser();

    // Send a updateParagraphResult message from the paragraph's new owner and verify that the ServiceCallback receives a null, as there is not an InterpreterGroup from which to search AngularObjects from.
    ServiceContext editedContext = new ServiceContext(new AuthenticationInfo(paragraphOwner),new HashSet<>());
    Assertions.assertDoesNotThrow(() -> notebookService.updateParagraphResult(noteId,paragraphId,"test-"+paragraphOwner,0,0,0,"",editedContext,callback));
    Assertions.assertDoesNotThrow(() -> verify(callback).onSuccess(null, editedContext));

    // Not deleting the notebook file after running the test will cause other tests to fail
    Path notebookPath = Paths.get(notebookDir.getPath(),note1.getPath()+"_"+noteId+".zpln");
    Assertions.assertDoesNotThrow(()->{Files.delete(notebookPath);});
  }

  @Test
  public void testNoteOperations() throws IOException {
    // get home note
    Note homeNote = notebookService.getHomeNote(context, callback);
    assertNull(homeNote);
    verify(callback).onSuccess(homeNote, context);

    // create note
    Note note1 = notebookService.createNote("/folder_1/note1", "test", true, context, callback);
    assertEquals("note1", note1.getName());
    assertEquals(1, note1.getParagraphCount());
    verify(callback).onSuccess(note1, context);

    // create duplicated note
    reset(callback);
    Note note2 = notebookService.createNote("/folder_1/note1", "test", true, context, callback);
    assertNull(note2);
    ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
    verify(callback).onFailure(exception.capture(), any(ServiceContext.class));
    assertEquals("Note '/folder_1/note1' existed", exception.getValue().getMessage());

    // list note
    reset(callback);
    List<NoteInfo> notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(1, notesInfo.size());
    assertEquals(note1.getId(), notesInfo.get(0).getId());
    assertEquals(note1.getName(), notesInfo.get(0).getNoteName());
    verify(callback).onSuccess(notesInfo, context);

    // get note
    reset(callback);
    Note note1_copy = notebookService.getNote(note1.getId(), context, callback);
    assertEquals(note1, note1_copy);
    verify(callback).onSuccess(note1_copy, context);

    // rename note
    reset(callback);
    notebookService.renameNote(note1.getId(), "/folder_2/new_name", false, context, callback);
    verify(callback).onSuccess(note1, context);
    assertEquals("new_name", note1.getName());

    // move folder
    reset(callback);
    notesInfo = notebookService.renameFolder("/folder_2", "/folder_3", context, callback);
    verify(callback).onSuccess(notesInfo, context);
    assertEquals(1, notesInfo.size());
    assertEquals("/folder_3/new_name", notesInfo.get(0).getPath());

    // move folder in case of folder path without prefix '/'
    reset(callback);
    notesInfo = notebookService.renameFolder("folder_3", "folder_4", context, callback);
    verify(callback).onSuccess(notesInfo, context);
    assertEquals(1, notesInfo.size());
    assertEquals("/folder_4/new_name", notesInfo.get(0).getPath());

    // create another note
    note2 = notebookService.createNote("/note2", "test", true, context, callback);
    assertEquals("note2", note2.getName());
    verify(callback).onSuccess(note2, context);

    // rename note
    reset(callback);
    notebookService.renameNote(note2.getId(), "new_note2", true, context, callback);
    verify(callback).onSuccess(note2, context);
    assertEquals("new_note2", note2.getName());

    // list note
    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(2, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);

    // delete note
    reset(callback);
    notebookService.removeNote(note2.getId(), context, callback);
    verify(callback).onSuccess("Delete note successfully", context);

    // list note again
    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(1, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);

    // delete folder
    notesInfo = notebookService.removeFolder("/folder_4", context, callback);
    verify(callback).onSuccess(notesInfo, context);

    // list note again
    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(0, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);

    // import note
    reset(callback);
    Note importedNote = notebookService.importNote("/Imported Note", "{}", context, callback);
    assertNotNull(importedNote);
    verify(callback).onSuccess(importedNote, context);

    // clone note
    reset(callback);
    Note clonedNote = notebookService.cloneNote(importedNote.getId(), "/Backup/Cloned Note",
        context, callback);
    assertEquals(importedNote.getParagraphCount(), clonedNote.getParagraphCount());
    verify(callback).onSuccess(clonedNote, context);

    // list note
    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(2, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);

    // test moving corrupted note to trash
    Note corruptedNote = notebookService.createNote("/folder_1/corruptedNote", "test", true, context, callback);
    String corruptedNotePath = notebookDir.getAbsolutePath() + corruptedNote.getPath() + "_" + corruptedNote.getId() + ".zpln";
    // corrupt note
    FileWriter myWriter = new FileWriter(corruptedNotePath);
    myWriter.write("{{{I'm corrupted;;;");
    myWriter.close();
    notebookService.moveNoteToTrash(corruptedNote.getId(), context, callback);
    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(3, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);
    notebookService.removeNote(corruptedNote.getId(), context, callback);

    // move note to Trash
    notebookService.moveNoteToTrash(importedNote.getId(), context, callback);

    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(2, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);

    boolean moveToTrash = false;
    for (NoteInfo noteInfo : notesInfo) {
      if (noteInfo.getId().equals(importedNote.getId())) {
        assertEquals("/~Trash/Imported Note", noteInfo.getPath());
        moveToTrash = true;
      }
    }
    assertTrue("No note is moved to trash", moveToTrash);

    // restore it
    notebookService.restoreNote(importedNote.getId(), context, callback);
    Note restoredNote = notebookService.getNote(importedNote.getId(), context, callback);
    assertNotNull(restoredNote);
    assertEquals("/Imported Note", restoredNote.getPath());

    // move it to Trash again
    notebookService.moveNoteToTrash(restoredNote.getId(), context, callback);

    // remove note from Trash
    reset(callback);

    notebookService.removeNote(importedNote.getId(), context, callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(1, notesInfo.size());

    // move folder to Trash
    notebookService.moveFolderToTrash("Backup", context, callback);

    reset(callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(1, notesInfo.size());
    verify(callback).onSuccess(notesInfo, context);
    moveToTrash = false;
    for (NoteInfo noteInfo : notesInfo) {
      if (noteInfo.getId().equals(clonedNote.getId())) {
        assertEquals("/~Trash/Backup/Cloned Note", noteInfo.getPath());
        moveToTrash = true;
      }
    }
    assertTrue("No folder is moved to trash", moveToTrash);

    // restore folder
    reset(callback);
    notebookService.restoreFolder("/~Trash/Backup", context, callback);
    restoredNote = notebookService.getNote(clonedNote.getId(), context, callback);
    assertNotNull(restoredNote);
    assertEquals("/Backup/Cloned Note", restoredNote.getPath());

    // move the folder to trash again
    notebookService.moveFolderToTrash("Backup", context, callback);

    // remove folder from Trash
    reset(callback);
    notebookService.removeFolder("/~Trash/Backup", context, callback);
    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(0, notesInfo.size());

    // empty trash
    notebookService.emptyTrash(context, callback);

    notesInfo = notebookService.listNotesInfo(false, context, callback);
    assertEquals(0, notesInfo.size());
  }

  @Test
  public void testRenameNoteRejectsDuplicate() throws IOException {
    Note note1 = notebookService.createNote("/folder/note1", "test", true, context, callback);
    assertEquals("note1", note1.getName());
    verify(callback).onSuccess(note1, context);

    reset(callback);
    Note note2 = notebookService.createNote("/folder/note2", "test", true, context, callback);
    assertEquals("note2", note2.getName());
    verify(callback).onSuccess(note2, context);

    reset(callback);
    ArgumentCaptor<NotePathAlreadyExistsException> exception = ArgumentCaptor.forClass(NotePathAlreadyExistsException.class);
    notebookService.renameNote(note1.getId(), "/folder/note2", false, context, callback);
    verify(callback).onFailure(exception.capture(), any(ServiceContext.class));
    assertEquals("Note '/folder/note2' existed", exception.getValue().getMessage());
    verify(callback, never()).onSuccess(any(), any());
  }


  @Test
  public void testParagraphOperations() throws IOException {
    // create note
    Note note1 = notebookService.createNote("note1", "python", false, context, callback);
    assertEquals("note1", note1.getName());
    assertEquals(0, note1.getParagraphCount());
    verify(callback).onSuccess(note1, context);

    // add paragraph
    reset(callback);
    Paragraph p = notebookService.insertParagraph(note1.getId(), 0, new HashMap<>(), context,
        callback);
    assertNotNull(p);
    verify(callback).onSuccess(p, context);
    assertEquals(1, note1.getParagraphCount());

    // update paragraph
    reset(callback);
    notebookService.updateParagraph(note1.getId(), p.getId(), "my_title", "my_text",
        new HashMap<>(), new HashMap<>(), context, callback);
    assertEquals("my_title", p.getTitle());
    assertEquals("my_text", p.getText());

    // move paragraph
    reset(callback);
    notebookService.moveParagraph(note1.getId(), p.getId(), 0, context, callback);
    assertEquals(p, note1.getParagraph(0));
    verify(callback).onSuccess(p, context);

    // run paragraph asynchronously
    reset(callback);
    p.getConfig().put("colWidth", "6.0");
    p.getConfig().put("title", true);
    boolean runStatus = notebookService.runParagraph(note1.getId(), p.getId(), "my_title", "1+1",
        new HashMap<>(), new HashMap<>(), null, false, false, context, callback);
    assertTrue(runStatus);
    verify(callback).onSuccess(p, context);
    assertEquals(2, p.getConfig().size());

    // run paragraph synchronously via correct code
    reset(callback);
    runStatus = notebookService.runParagraph(note1.getId(), p.getId(), "my_title", "1+1",
        new HashMap<>(), new HashMap<>(), null, false, true, context, callback);
    assertTrue(runStatus);
    verify(callback).onSuccess(p, context);
    assertEquals(2, p.getConfig().size());

    // run all paragraphs, with null paragraph list provided
    reset(callback);
    assertTrue(notebookService.runAllParagraphs(
            note1.getId(),
            null,
            context, callback));

    reset(callback);
    runStatus = notebookService.runParagraph(note1.getId(), p.getId(), "my_title", "invalid_code",
        new HashMap<>(), new HashMap<>(), null, false, true, context, callback);
    assertTrue(runStatus);
    // TODO(zjffdu) Enable it after ZEPPELIN-3699
    // assertNotNull(p.getResult());
    verify(callback).onSuccess(p, context);

    // clean output
    reset(callback);
    notebookService.clearParagraphOutput(note1.getId(), p.getId(), context, callback);
    assertNull(p.getReturn());
    verify(callback).onSuccess(p, context);
  }

  @Test
  public void testNormalizeNotePath() throws IOException {
    assertEquals("/Untitled Note", notebookService.normalizeNotePath(" "));
    assertEquals("/Untitled Note", notebookService.normalizeNotePath(null));
    assertEquals("/my_note", notebookService.normalizeNotePath("my_note"));
    assertEquals("/my  note", notebookService.normalizeNotePath("my\r\nnote"));

    try {
      String longNoteName = StringUtils.join(
          IntStream.range(0, 256).boxed().collect(Collectors.toList()), "");
      notebookService.normalizeNotePath(longNoteName);
      fail("Should fail");
    } catch (IOException e) {
      assertEquals("Note name must be less than 255", e.getMessage());
    }
    try {
      notebookService.normalizeNotePath("my..note");
      fail("Should fail");
    } catch (IOException e) {
      assertEquals("Note name can not contain '..'", e.getMessage());
    }
  }
}
