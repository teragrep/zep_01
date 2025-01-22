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
package org.apache.zeppelin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.zeppelin.notebook.AuthorizationService;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.rest.message.NoteJobStatus;
import org.apache.zeppelin.user.AuthenticationInfoImpl;
import org.apache.zeppelin.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.user.AuthenticationInfo;

/**
 * BASIC Zeppelin rest api tests.
 */
@Ignore(value="Flaky tests: HttpHostConnect Connect to localhost:8080 [localhost/127.0.0.1] failed: Connection refused (Connection refused) or Task org.apache.zeppelin.notebook.NoteEventAsyncListener$EventHandling@1eea9d2d rejected from java.util.concurrent.ThreadPoolExecutor@29182679")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZeppelinRestApiTest extends AbstractTestRestApi {
  Gson gson = new Gson();
  AuthenticationInfo anonymous;

  @BeforeClass
  public static void init() throws Exception {
    AbstractTestRestApi.startUp(ZeppelinRestApiTest.class.getSimpleName());
  }

  @AfterClass
  public static void destroy() throws Exception {
    AbstractTestRestApi.shutDown();
  }

  @Before
  public void setUp() {
    anonymous = new AuthenticationInfoImpl("anonymous");
  }

  /**
   * ROOT API TEST.
   **/
  @Test
  public void getApiRoot() throws IOException {
    // when
    CloseableHttpResponse httpGetRoot = httpGet("/");
    // then
    assertThat(httpGetRoot, isAllowed());
    httpGetRoot.close();
  }

  @Test
  public void testGetNoteInfo() throws IOException {
    LOG.debug("testGetNoteInfo");
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("note");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);
    String paragraphText = "%md This is my new paragraph in my new note";
    paragraph.setText(paragraphText);
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);

    String sourceNoteId = note.getId();
    CloseableHttpResponse get = httpGet("/notebook/" + sourceNoteId);
    String getResponse = EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testGetNoteInfo \n" + getResponse);
    assertThat("test note get method:", get, isAllowed());

    Map<String, Object> resp = gson.fromJson(getResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    assertNotNull(resp);
    assertEquals("OK", resp.get("status"));

    Map<String, Object> body = (Map<String, Object>) resp.get("body");
    List<Map<String, Object>> paragraphs = (List<Map<String, Object>>) body.get("paragraphs");

    assertTrue(paragraphs.size() > 0);
    assertEquals(paragraphText, paragraphs.get(0).get("text"));
    get.close();
  }

  @Test
  public void testNoteCreateWithName() throws IOException {
    String noteName = "Test note name";
    testNoteCreate(noteName);
  }

  @Test
  public void testNoteCreateNoName() throws IOException {
    testNoteCreate("");
  }

  @Test
  public void testNoteCreateWithParagraphs() throws IOException {
    // Call Create Note REST API
    String noteName = "test";
    String jsonRequest = "{\"name\":\"" + noteName + "\", \"paragraphs\": [" +
        "{\"title\": \"title1\", \"text\": \"text1\"}," +
        "{\"title\": \"title2\", \"text\": \"text2\"}," +
        "{\"title\": \"titleConfig\", \"text\": \"text3\", " +
        "\"config\": {\"colWidth\": 9.0, \"title\": true, " +
        "\"results\": [{\"graph\": {\"mode\": \"pieChart\"}}] " +
        "}}]} ";
    CloseableHttpResponse post = httpPost("/notebook/", jsonRequest);
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testNoteCreate \n" + postResponse);
    assertThat("test note create method:", post, isAllowed());

    Map<String, Object> resp = gson.fromJson(postResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    String newNoteId =  (String) resp.get("body");
    LOG.debug("newNoteId:=" + newNoteId);
    Note newNote = TestUtils.getInstance(Notebook.class).getNote(newNoteId);
    assertNotNull("Can not find new note by id", newNote);
    // This is partial test as newNote is in memory but is not persistent
    String newNoteName = newNote.getName();
    LOG.debug("new note name is: " + newNoteName);
    String expectedNoteName = noteName;
    if (noteName.isEmpty()) {
      expectedNoteName = "Note " + newNoteId;
    }
    assertEquals("compare note name", expectedNoteName, newNoteName);
    assertEquals("initial paragraph check failed", 3, newNote.getParagraphs().size());
    for (Paragraph p : newNote.getParagraphs()) {
      if (StringUtils.isEmpty(p.getText())) {
        continue;
      }
      assertTrue("paragraph title check failed", p.getTitle().startsWith("title"));
      assertTrue("paragraph text check failed", p.getText().startsWith("text"));
      if (p.getTitle().equals("titleConfig")) {
        assertEquals("paragraph col width check failed", 9.0, p.getConfig().get("colWidth"));
        assertTrue("paragraph show title check failed", ((boolean) p.getConfig().get("title")));
        Map graph = ((List<Map>) p.getConfig().get("results")).get(0);
        String mode = ((Map) graph.get("graph")).get("mode").toString();
        assertEquals("paragraph graph mode check failed", "pieChart", mode);
      }
    }
    // cleanup
    post.close();
  }

  private void testNoteCreate(String noteName) throws IOException {
    // Call Create Note REST API
    String jsonRequest = "{\"name\":\"" + noteName + "\"}";
    CloseableHttpResponse post = httpPost("/notebook/", jsonRequest);
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testNoteCreate \n" + postResponse);
    assertThat("test note create method:", post, isAllowed());

    Map<String, Object> resp = gson.fromJson(postResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    String newNoteId =  (String) resp.get("body");
    LOG.debug("newNoteId:=" + newNoteId);
    Note newNote = TestUtils.getInstance(Notebook.class).getNote(newNoteId);
    assertNotNull("Can not find new note by id", newNote);
    // This is partial test as newNote is in memory but is not persistent
    String newNoteName = newNote.getName();
    LOG.debug("new note name is: " + newNoteName);
    if (StringUtils.isBlank(noteName)) {
      noteName = "Untitled Note";
    }
    assertEquals("compare note name", noteName, newNoteName);
    // cleanup
    post.close();
  }

  @Test
  public void testDeleteNote() throws IOException {
    LOG.debug("testDeleteNote");
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testDeletedNote", anonymous);
    String noteId = note.getId();
    testDeleteNote(noteId);
  }

  @Test
  public void testDeleteNoteBadId() throws IOException {
    LOG.debug("testDeleteNoteBadId");
    testDeleteNotExistNote("bad_ID");
  }

  @Test
  public void testExportNote() throws IOException {
    LOG.debug("testExportNote");

    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testExportNote", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("source note for export");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);
    paragraph.setText("%md This is my new paragraph in my new note");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String sourceNoteId = note.getId();
    // Call export Note REST API
    CloseableHttpResponse get = httpGet("/notebook/export/" + sourceNoteId);
    String getResponse = EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testNoteExport \n" + getResponse);
    assertThat("test note export method:", get, isAllowed());

    Map<String, Object> resp =
        gson.fromJson(getResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    String exportJSON = (String) resp.get("body");
    assertNotNull("Can not find new notejson", exportJSON);
    LOG.debug("export JSON:=" + exportJSON);
    get.close();
  }

  @Test
  public void testImportNotebook() throws IOException {
    Map<String, Object> resp;
    String oldJson;
    String noteName;
    noteName = "source note for import";
    LOG.debug("testImportNote");
    // create test note
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testImportNotebook", anonymous);
    assertNotNull("can't create new note", note);
    note.setName(noteName);
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);
    paragraph.setText("%md This is my new paragraph in my new note");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String sourceNoteId = note.getId();
    // get note content as JSON
    oldJson = getNoteContent(sourceNoteId);
    // delete it first then import it
    TestUtils.getInstance(Notebook.class).removeNote(note, anonymous);

    // call note post
    CloseableHttpResponse importPost = httpPost("/notebook/import/", oldJson);
    assertThat(importPost, isAllowed());
    resp =
        gson.fromJson(EntityUtils.toString(importPost.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    String importId = (String) resp.get("body");

    assertNotNull("Did not get back a note id in body", importId);
    Note newNote = TestUtils.getInstance(Notebook.class).getNote(importId);
    assertEquals("Compare note names", noteName, newNote.getName());
    assertEquals("Compare paragraphs count", note.getParagraphs().size(), newNote.getParagraphs()
        .size());
    importPost.close();
  }

  private String getNoteContent(String id) throws IOException {
    CloseableHttpResponse get = httpGet("/notebook/export/" + id);
    assertThat(get, isAllowed());
    Map<String, Object> resp =
        gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals(200, get.getStatusLine().getStatusCode());
    String body = resp.get("body").toString();
    // System.out.println("Body is " + body);
    get.close();
    return body;
  }

  private void testDeleteNote(String noteId) throws IOException {
    CloseableHttpResponse delete = httpDelete(("/notebook/" + noteId));
    LOG.debug("testDeleteNote delete response\n" + EntityUtils.toString(delete.getEntity(), StandardCharsets.UTF_8));
    assertThat("Test delete method:", delete, isAllowed());
    delete.close();
    // make sure note is deleted
    Note deletedNote = TestUtils.getInstance(Notebook.class).getNote(noteId);
    assertNull("Deleted note should be null", deletedNote);
  }

  private void testDeleteNotExistNote(String noteId) throws IOException {
    CloseableHttpResponse delete = httpDelete(("/notebook/" + noteId));
    LOG.debug("testDeleteNote delete response\n" + EntityUtils.toString(delete.getEntity(), StandardCharsets.UTF_8));
    assertThat("Test delete method:", delete, isNotFound());
    delete.close();
  }

  @Test
  public void testCloneNote() throws IOException, IllegalArgumentException {
    LOG.debug("testCloneNote");
    // Create note to clone
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testCloneNote", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("source note for clone");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);
    paragraph.setText("%md This is my new paragraph in my new note");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String sourceNoteId = note.getId();

    String noteName = "clone Note Name";
    // Call Clone Note REST API
    String jsonRequest = "{\"name\":\"" + noteName + "\"}";
    CloseableHttpResponse post = httpPost("/notebook/" + sourceNoteId, jsonRequest);
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testNoteClone \n" + postResponse);
    assertThat("test note clone method:", post, isAllowed());

    Map<String, Object> resp = gson.fromJson(postResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    String newNoteId =  (String) resp.get("body");
    LOG.debug("newNoteId:=" + newNoteId);
    Note newNote = TestUtils.getInstance(Notebook.class).getNote(newNoteId);
    assertNotNull("Can not find new note by id", newNote);
    assertEquals("Compare note names", noteName, newNote.getName());
    assertEquals("Compare paragraphs count", note.getParagraphs().size(),
            newNote.getParagraphs().size());
    post.close();
  }

  @Test
  public void testListNotes() throws IOException {
    LOG.debug("testListNotes");
    CloseableHttpResponse get = httpGet("/notebook/");
    assertThat("List notes method", get, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    List<Map<String, String>> body = (List<Map<String, String>>) resp.get("body");
    //TODO(khalid): anonymous or specific user notes?
    HashSet<String> anonymous = new HashSet<>(Arrays.asList("anonymous"));
    AuthorizationService authorizationService = TestUtils.getInstance(AuthorizationService.class);
    assertEquals("List notes are equal", TestUtils.getInstance(Notebook.class)
            .getAllNotes(note -> authorizationService.isReader(note.getId(), anonymous))
            .size(), body.size());
    get.close();
  }

  @Test
  public void testNoteJobs() throws Exception {
    LOG.debug("testNoteJobs");

    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testNoteJobs", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);

    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);

    paragraph.setText("%md This is test paragraph.");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String noteId = note.getId();

    note.runAll(anonymous, true, false, new HashMap<>());
    // wait until job is finished or timeout.
    int timeout = 1;
    while (!paragraph.isTerminated()) {
      Thread.sleep(1000);
      if (timeout++ > 10) {
        fail("Failure: testNoteJobs timeout job.");
        break;
      }
    }

    // Call Run note jobs REST API
    CloseableHttpResponse postNoteJobs = httpPost("/notebook/job/" + noteId + "?blocking=true", "");
    assertThat("test note jobs run:", postNoteJobs, isAllowed());
    postNoteJobs.close();

    // Call Stop note jobs REST API
    CloseableHttpResponse deleteNoteJobs = httpDelete("/notebook/job/" + noteId);
    assertThat("test note stop:", deleteNoteJobs, isAllowed());
    deleteNoteJobs.close();
    Thread.sleep(1000);

    // Call Run paragraph REST API
    CloseableHttpResponse postParagraph = httpPost("/notebook/job/" + noteId + "/" + paragraph.getId(), "");
    assertThat("test paragraph run:", postParagraph, isAllowed());
    postParagraph.close();
    Thread.sleep(1000);

    // Call Stop paragraph REST API
    CloseableHttpResponse deleteParagraph = httpDelete("/notebook/job/" + noteId + "/" + paragraph.getId());
    assertThat("test paragraph stop:", deleteParagraph, isAllowed());
    deleteParagraph.close();
  }

  @Test
  public void testGetNoteJob() throws Exception {
    LOG.debug("testGetNoteJob");

    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testGetNoteJob", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);

    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);

    paragraph.setText("%sh sleep 1");
    paragraph.setAuthenticationInfo(anonymous);
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String noteId = note.getId();

    note.runAll(anonymous, true, false, new HashMap<>());
    // assume that status of the paragraph is running
    CloseableHttpResponse get = httpGet("/notebook/job/" + noteId);
    assertThat("test get note job: ", get, isAllowed());
    String responseBody = EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8);
    get.close();

    LOG.debug("test get note job: \n" + responseBody);
    Map<String, Object> resp = gson.fromJson(responseBody,
            new TypeToken<Map<String, Object>>() {}.getType());

    NoteJobStatus noteJobStatus = NoteJobStatus.fromJson(gson.toJson(resp.get("body")));
    assertEquals(1, noteJobStatus.getParagraphJobStatusList().size());
    int progress = Integer.parseInt(noteJobStatus.getParagraphJobStatusList().get(0).getProgress());
    assertTrue(progress >= 0 && progress <= 100);

    // wait until job is finished or timeout.
    int timeout = 1;
    while (!paragraph.isTerminated()) {
      Thread.sleep(100);
      if (timeout++ > 10) {
        fail("Failure: testGetNoteJob timeout job.");
        break;
      }
    }
  }

  @Test
  public void testRunParagraphWithParams() throws Exception {
    LOG.debug("testRunParagraphWithParams");

    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testRunParagraphWithParams", anonymous);
    assertNotNull("can't create new note", note);
    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);

    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);

    paragraph.setText("%spark\nval param = z.input(\"param\").toString\nprintln(param)");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);
    String noteId = note.getId();

    note.runAll(anonymous, true, false, new HashMap<>());

    // Call Run paragraph REST API
    CloseableHttpResponse postParagraph = httpPost("/notebook/job/" + noteId + "/" + paragraph.getId(),
        "{\"params\": {\"param\": \"hello\", \"param2\": \"world\"}}");
    assertThat("test paragraph run:", postParagraph, isAllowed());
    postParagraph.close();
    Thread.sleep(1000);

    Note retrNote = TestUtils.getInstance(Notebook.class).getNote(noteId);
    Paragraph retrParagraph = retrNote.getParagraph(paragraph.getId());
    Map<String, Object> params = retrParagraph.settings.getParams();
    assertEquals("hello", params.get("param"));
    assertEquals("world", params.get("param2"));
  }

  @Test
  public void testJobs() throws Exception {
    // create a note and a paragraph
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_CRON_ENABLE.getVarName(), "true");
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testJobs", anonymous);

    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    paragraph.setText("%md This is test paragraph.");

    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);

    note.runAll(AuthenticationInfoImpl.ANONYMOUS, false, false, new HashMap<>());

    String jsonRequest = "{\"cron\":\"* * * * * ?\" }";
    // right cron expression but not exist note.
    CloseableHttpResponse postCron = httpPost("/notebook/cron/notexistnote", jsonRequest);
    assertThat("", postCron, isNotFound());
    postCron.close();

    // right cron expression.
    postCron = httpPost("/notebook/cron/" + note.getId(), jsonRequest);
    assertThat("", postCron, isAllowed());
    postCron.close();
    Thread.sleep(1000);

    // wrong cron expression.
    jsonRequest = "{\"cron\":\"a * * * * ?\" }";
    postCron = httpPost("/notebook/cron/" + note.getId(), jsonRequest);
    assertThat("", postCron, isBadRequest());
    postCron.close();
    Thread.sleep(1000);

    // remove cron job.
    CloseableHttpResponse deleteCron = httpDelete("/notebook/cron/" + note.getId());
    assertThat("", deleteCron, isAllowed());
    deleteCron.close();
  }

  @Test
  public void testCronDisable() throws Exception {
    // create a note and a paragraph
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_CRON_ENABLE.getVarName(), "false");
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testCronDisable", anonymous);

    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    paragraph.setText("%md This is test paragraph.");

    Map<String, Object> config = paragraph.getConfig();
    config.put("enabled", true);
    paragraph.setConfig(config);

    note.runAll(AuthenticationInfoImpl.ANONYMOUS, true, true, new HashMap<>());

    String jsonRequest = "{\"cron\":\"* * * * * ?\" }";
    // right cron expression.
    CloseableHttpResponse postCron = httpPost("/notebook/cron/" + note.getId(), jsonRequest);
    assertThat("", postCron, isForbidden());
    postCron.close();

    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_CRON_ENABLE.getVarName(), "true");
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_CRON_FOLDERS.getVarName(), "/System");

    note.setName("System/test2");
    note.runAll(AuthenticationInfoImpl.ANONYMOUS, true, true, new HashMap<>());
    postCron = httpPost("/notebook/cron/" + note.getId(), jsonRequest);
    assertThat("", postCron, isAllowed());
    postCron.close();
    Thread.sleep(1000);

    // remove cron job.
    CloseableHttpResponse deleteCron = httpDelete("/notebook/cron/" + note.getId());
    assertThat("", deleteCron, isAllowed());
    deleteCron.close();
    Thread.sleep(1000);

    System.clearProperty(ConfVars.ZEPPELIN_NOTEBOOK_CRON_FOLDERS.getVarName());
  }

  @Test
  public void testRegressionZEPPELIN_527() throws Exception {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testRegressionZEPPELIN_527", anonymous);
    note.setName("note for run test");
    Paragraph paragraph = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    paragraph.setText("%spark\nval param = z.input(\"param\").toString\nprintln(param)");
    note.runAll(AuthenticationInfoImpl.ANONYMOUS, true, false, new HashMap<>());
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);

    CloseableHttpResponse getNoteJobs = httpGet("/notebook/job/" + note.getId());
    assertThat("test note jobs run:", getNoteJobs, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(getNoteJobs.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    NoteJobStatus noteJobStatus = NoteJobStatus.fromJson(gson.toJson(resp.get("body")));
    assertNotNull(noteJobStatus.getParagraphJobStatusList().get(0).getStarted());
    assertNotNull(noteJobStatus.getParagraphJobStatusList().get(0).getFinished());
    getNoteJobs.close();
  }

  @Test
  public void testInsertParagraph() throws IOException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testInsertParagraph", anonymous);

    String jsonRequest = "{\"title\": \"title1\", \"text\": \"text1\"}";
    CloseableHttpResponse post = httpPost("/notebook/" + note.getId() + "/paragraph", jsonRequest);
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testInsertParagraph response\n" + postResponse);
    assertThat("Test insert method:", post, isAllowed());
    post.close();

    Map<String, Object> resp = gson.fromJson(postResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    String newParagraphId = (String) resp.get("body");
    LOG.debug("newParagraphId:=" + newParagraphId);

    Note retrNote = TestUtils.getInstance(Notebook.class).getNote(note.getId());
    Paragraph newParagraph = retrNote.getParagraph(newParagraphId);
    assertNotNull("Can not find new paragraph by id", newParagraph);

    assertEquals("title1", newParagraph.getTitle());
    assertEquals("text1", newParagraph.getText());

    Paragraph lastParagraph = note.getLastParagraph();
    assertEquals(newParagraph.getId(), lastParagraph.getId());

    // insert to index 0
    String jsonRequest2 = "{\"index\": 0, \"title\": \"title2\", \"text\": \"text2\"}";
    CloseableHttpResponse post2 = httpPost("/notebook/" + note.getId() + "/paragraph", jsonRequest2);
    LOG.debug("testInsertParagraph response2\n" + EntityUtils.toString(post2.getEntity(), StandardCharsets.UTF_8));
    assertThat("Test insert method:", post2, isAllowed());
    post2.close();

    Paragraph paragraphAtIdx0 = note.getParagraphs().get(0);
    assertEquals("title2", paragraphAtIdx0.getTitle());
    assertEquals("text2", paragraphAtIdx0.getText());

    //append paragraph providing graph
    String jsonRequest3 = "{\"title\": \"title3\", \"text\": \"text3\", " +
                          "\"config\": {\"colWidth\": 9.0, \"title\": true, " +
                          "\"results\": [{\"graph\": {\"mode\": \"pieChart\"}}]}}";
    CloseableHttpResponse post3 = httpPost("/notebook/" + note.getId() + "/paragraph", jsonRequest3);
    LOG.debug("testInsertParagraph response4\n" + EntityUtils.toString(post3.getEntity(), StandardCharsets.UTF_8));
    assertThat("Test insert method:", post3, isAllowed());
    post3.close();

    Paragraph p = note.getLastParagraph();
    assertEquals("title3", p.getTitle());
    assertEquals("text3", p.getText());
    Map result = ((List<Map>) p.getConfig().get("results")).get(0);
    String mode = ((Map) result.get("graph")).get("mode").toString();
    assertEquals("pieChart", mode);
    assertEquals(9.0, p.getConfig().get("colWidth"));
    assertTrue(((boolean) p.getConfig().get("title")));
  }

  @Test
  public void testUpdateParagraph() throws IOException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testUpdateParagraph", anonymous);

    String jsonRequest = "{\"title\": \"title1\", \"text\": \"text1\"}";
    CloseableHttpResponse post = httpPost("/notebook/" + note.getId() + "/paragraph", jsonRequest);
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    post.close();

    String newParagraphId = (String) resp.get("body");
    Paragraph newParagraph = TestUtils.getInstance(Notebook.class).getNote(note.getId())
            .getParagraph(newParagraphId);

    assertEquals("title1", newParagraph.getTitle());
    assertEquals("text1", newParagraph.getText());

    String updateRequest = "{\"text\": \"updated text\"}";
    CloseableHttpResponse put = httpPut("/notebook/" + note.getId() + "/paragraph/" + newParagraphId,
            updateRequest);
    assertThat("Test update method:", put, isAllowed());
    put.close();

    Paragraph updatedParagraph = TestUtils.getInstance(Notebook.class).getNote(note.getId())
            .getParagraph(newParagraphId);

    assertEquals("title1", updatedParagraph.getTitle());
    assertEquals("updated text", updatedParagraph.getText());

    String updateBothRequest = "{\"title\": \"updated title\", \"text\" : \"updated text 2\" }";
    CloseableHttpResponse updatePut = httpPut("/notebook/" + note.getId() + "/paragraph/" + newParagraphId,
            updateBothRequest);
    updatePut.close();

    Paragraph updatedBothParagraph = TestUtils.getInstance(Notebook.class).getNote(note.getId())
            .getParagraph(newParagraphId);

    assertEquals("updated title", updatedBothParagraph.getTitle());
    assertEquals("updated text 2", updatedBothParagraph.getText());
  }

  @Test
  public void testGetParagraph() throws IOException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testGetParagraph", anonymous);

    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p.setTitle("hello");
    p.setText("world");
    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);

    CloseableHttpResponse get = httpGet("/notebook/" + note.getId() + "/paragraph/" + p.getId());
    String getResponse = EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testGetParagraph response\n" + getResponse);
    assertThat("Test get method: ", get, isAllowed());
    get.close();

    Map<String, Object> resp = gson.fromJson(getResponse,
            new TypeToken<Map<String, Object>>() {}.getType());

    assertNotNull(resp);
    assertEquals("OK", resp.get("status"));

    Map<String, Object> body = (Map<String, Object>) resp.get("body");

    assertEquals(p.getId(), body.get("id"));
    assertEquals("hello", body.get("title"));
    assertEquals("world", body.get("text"));
  }

  @Test
  public void testMoveParagraph() throws IOException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testMoveParagraph", anonymous);

    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p.setTitle("title1");
    p.setText("text1");

    Paragraph p2 = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p2.setTitle("title2");
    p2.setText("text2");

    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);

    CloseableHttpResponse post = httpPost("/notebook/" + note.getId() + "/paragraph/" + p2.getId() +
            "/move/" + 0, "");
    assertThat("Test post method: ", post, isAllowed());
    post.close();

    Note retrNote = TestUtils.getInstance(Notebook.class).getNote(note.getId());
    Paragraph paragraphAtIdx0 = retrNote.getParagraphs().get(0);

    assertEquals(p2.getId(), paragraphAtIdx0.getId());
    assertEquals(p2.getTitle(), paragraphAtIdx0.getTitle());
    assertEquals(p2.getText(), paragraphAtIdx0.getText());

    CloseableHttpResponse post2 = httpPost("/notebook/" + note.getId() + "/paragraph/" + p2.getId() +
            "/move/" + 10, "");
    assertThat("Test post method: ", post2, isBadRequest());
    post.close();
  }

  @Test
  public void testDeleteParagraph() throws IOException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testDeleteParagraph", anonymous);

    Paragraph p = note.addNewParagraph(AuthenticationInfoImpl.ANONYMOUS);
    p.setTitle("title1");
    p.setText("text1");

    TestUtils.getInstance(Notebook.class).saveNote(note, anonymous);

    CloseableHttpResponse delete = httpDelete("/notebook/" + note.getId() + "/paragraph/" + p.getId());
    assertThat("Test delete method: ", delete, isAllowed());
    delete.close();

    Note retrNote = TestUtils.getInstance(Notebook.class).getNote(note.getId());
    Paragraph retrParagrah = retrNote.getParagraph(p.getId());
    assertNull("paragraph should be deleted", retrParagrah);
  }

  @Test
  public void testTitleSearch() throws IOException, InterruptedException {
    Note note = TestUtils.getInstance(Notebook.class).createNote("note1_testTitleSearch", anonymous);
    String jsonRequest = "{\"title\": \"testTitleSearchOfParagraph\", " +
            "\"text\": \"ThisIsToTestSearchMethodWithTitle \"}";
    CloseableHttpResponse postNoteText = httpPost("/notebook/" + note.getId() + "/paragraph", jsonRequest);
    postNoteText.close();
    Thread.sleep(3000);

    CloseableHttpResponse searchNote = httpGet("/notebook/search?q='testTitleSearchOfParagraph'");
    Map<String, Object> respSearchResult = gson.fromJson(EntityUtils.toString(searchNote.getEntity(), StandardCharsets.UTF_8),
        new TypeToken<Map<String, Object>>() {
        }.getType());
    ArrayList searchBody = (ArrayList) respSearchResult.get("body");

    int numberOfTitleHits = 0;
    for (int i = 0; i < searchBody.size(); i++) {
      Map<String, String> searchResult = (Map<String, String>) searchBody.get(i);
      if (searchResult.get("header").contains("testTitleSearchOfParagraph")) {
        numberOfTitleHits++;
      }
    }
    assertTrue("Paragraph title hits must be at-least one", numberOfTitleHits >= 1);
    searchNote.close();
  }
}
