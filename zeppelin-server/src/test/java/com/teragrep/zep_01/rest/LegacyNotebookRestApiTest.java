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
package com.teragrep.zep_01.rest;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.InterpreterSettingManager;
import com.teragrep.zep_01.notebook.LegacyNotebook;
import com.teragrep.zep_01.rest.message.ParametersRequest;
import com.teragrep.zep_01.socket.NotebookServer;
import com.teragrep.zep_01.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.LegacyParagraph;
import com.teragrep.zep_01.scheduler.Job;
import com.teragrep.zep_01.user.AuthenticationInfo;

/**
 * Zeppelin notebook rest api tests.
 */
@Ignore(value="Flaky tests: HttpHostConnect Connect to localhost:8080 [localhost/127.0.0.1] failed: Connection refused (Connection refused)")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LegacyNotebookRestApiTest extends AbstractTestRestApi {
  Gson gson = new Gson();
  AuthenticationInfo anonymous;

  @BeforeClass
  public static void init() throws Exception {
    startUp(LegacyNotebookRestApiTest.class.getSimpleName());
    TestUtils.getInstance(LegacyNotebook.class).setParagraphJobListener(NotebookServer.getInstance());
  }

  @AfterClass
  public static void destroy() throws Exception {
    AbstractTestRestApi.shutDown();
  }

  @Before
  public void setUp() {
    anonymous = new AuthenticationInfo("anonymous");
  }

  @Test
  public void testGetReloadNote() throws IOException {
    LOG.debug("Running testGetNote");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    TestUtils.getInstance(LegacyNotebook.class).saveNote(note1, anonymous);
    CloseableHttpResponse get = httpGet("/notebook/" + note1.getId());
    assertThat(get, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Object> noteObject = (Map<String, Object>) resp.get("body");
    assertEquals(1, ((List)noteObject.get("paragraphs")).size());

    // add one new paragraph, but don't save it and reload it again
    note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    get = httpGet("/notebook/" + note1.getId() + "?reload=true");
    assertThat(get, isAllowed());
    resp = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    noteObject = (Map<String, Object>) resp.get("body");
    assertEquals(1, ((List)noteObject.get("paragraphs")).size());
    get.close();
  }

  @Test
  public void testGetNoteParagraphJobStatus() throws IOException {
    LOG.debug("Running testGetNoteParagraphJobStatus");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    String paragraphId = note1.getLastParagraph().getId();

    CloseableHttpResponse get = httpGet("/notebook/job/" + note1.getId() + "/" + paragraphId);
    assertThat(get, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Set<String>> paragraphStatus = (Map<String, Set<String>>) resp.get("body");

    // Check id and status have proper value
    assertEquals(paragraphStatus.get("id"), paragraphId);
    assertEquals("READY", paragraphStatus.get("status"));
    get.close();
  }

  @Test
  public void testRunParagraphJob() throws Exception {
    LOG.debug("Running testRunParagraphJob");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    LegacyParagraph p = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    // run blank paragraph
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "/" + p.getId(), "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    p.waitUntilFinished();
    assertEquals(Job.Status.FINISHED, p.getStatus());

    // run non-blank paragraph
    p.setText("test");
    post = httpPost("/notebook/job/" + note1.getId() + "/" + p.getId(), "");
    assertThat(post, isAllowed());
    resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    p.waitUntilFinished();
    assertNotEquals(Job.Status.FINISHED, p.getStatus());
  }

  @Test
  public void testRunParagraphSynchronously() throws IOException {
    LOG.debug("Running testRunParagraphSynchronously");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    LegacyParagraph p = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);

    // run non-blank paragraph
    String title = "title";
    String text = "%sh\n sleep 1";
    p.setTitle(title);
    p.setText(text);

    CloseableHttpResponse post = httpPost("/notebook/run/" + note1.getId() + "/" + p.getId(), "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
        new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    assertNotEquals(Job.Status.READY, p.getStatus());

    // Check if the paragraph is emptied
    assertEquals(title, p.getTitle());
    assertEquals(text, p.getText());

    // run invalid code
    text = "%sh\n invalid_cmd";
    p.setTitle(title);
    p.setText(text);

    post = httpPost("/notebook/run/" + note1.getId() + "/" + p.getId(), "");
    assertEquals(200, post.getStatusLine().getStatusCode());
    resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    Map stringMap = (Map) resp.get("body");
    assertEquals("ERROR", stringMap.get("code"));
    List<Map> interpreterResults = (List<Map>) stringMap.get("msg");
    assertTrue(interpreterResults.get(0).toString(),
            interpreterResults.get(0).get("data").toString().contains("invalid_cmd: command not found"));
    post.close();
    assertNotEquals(Job.Status.READY, p.getStatus());

    // Check if the paragraph is emptied
    assertEquals(title, p.getTitle());
    assertEquals(text, p.getText());
  }

  @Test
  public void testCreateNote() throws Exception {
    LOG.debug("Running testCreateNote");
    String message1 = "{\n\t\"name\" : \"test1\",\n\t\"addingEmptyParagraph\" : true\n}";
    CloseableHttpResponse post1 = httpPost("/notebook/", message1);
    assertThat(post1, isAllowed());

    Map<String, Object> resp1 = gson.fromJson(EntityUtils.toString(post1.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp1.get("status"));

    String noteId1 = (String) resp1.get("body");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(noteId1);
    assertEquals("test1", note1.getName());
    assertEquals(1, note1.getParagraphCount());
    assertNull(note1.getParagraph(0).getText());
    assertNull(note1.getParagraph(0).getTitle());

    String message2 = "{\n\t\"name\" : \"test2\"\n}";
    CloseableHttpResponse post2 = httpPost("/notebook/", message2);
    assertThat(post2, isAllowed());

    Map<String, Object> resp2 = gson.fromJson(EntityUtils.toString(post2.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp2.get("status"));

    String noteId2 = (String) resp2.get("body");
    Note note2 = TestUtils.getInstance(LegacyNotebook.class).getNote(noteId2);
    assertEquals("test2", note2.getName());
    assertEquals(0, note2.getParagraphCount());
  }

  @Test
  public void testRunNoteBlocking() throws IOException {
    LOG.debug("Running testRunNoteBlocking");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
      // 2 paragraphs
      // P1:
      //    %python
      //    from __future__ import print_function
      //    import time
      //    time.sleep(1)
      //    user='abc'
      // P2:
      //    %python
      //    print(user)
      //
      LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
      LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
      p1.setText("%python from __future__ import print_function\nimport time\ntime.sleep(1)\nuser='abc'");
      p2.setText("%python print(user)");

      CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
      assertThat(post, isAllowed());
      Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
              new TypeToken<Map<String, Object>>() {}.getType());
      assertEquals("OK", resp.get("status"));
      post.close();

      assertEquals(Job.Status.FINISHED, p1.getStatus());
      assertEquals(Job.Status.FINISHED, p2.getStatus());
      assertEquals("abc\n", p2.getReturn().message().get(0).getData());
  }

  @Ignore(value="This test should not work as %sh should not be available")
  @Test
  public void testRunNoteNonBlocking() throws Exception {
    LOG.debug("Running testRunNoteNonBlocking");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    import time
    //    time.sleep(5)
    //    name='hello'
    //    z.put('name', name)
    // P2:
    //    %%sh(interpolate=true)
    //    echo '{name}'
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python import time\ntime.sleep(5)\nname='hello'\nz.put('name', name)");
    p2.setText("%sh(interpolate=true) echo '{name}'");

    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();

    p1.waitUntilFinished();
    p2.waitUntilFinished();

    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.FINISHED, p2.getStatus());
    assertEquals("hello\n", p2.getReturn().message().get(0).getData());
  }

  @Test
  public void testRunNoteBlocking_Isolated() throws IOException {
    LOG.debug("Running testRunNoteBlocking_Isolated");
    InterpreterSettingManager interpreterSettingManager =
            TestUtils.getInstance(InterpreterSettingManager.class);
    InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("python");
    int pythonProcessNum = interpreterSetting.getAllInterpreterGroups().size();

    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    from __future__ import print_function
    //    import time
    //    time.sleep(1)
    //    user='abc'
    // P2:
    //    %python
    //    print(user)
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python from __future__ import print_function\nimport time\ntime.sleep(1)\nuser='abc'");
    p2.setText("%python print(user)");

    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true&isolated=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();

    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.FINISHED, p2.getStatus());
    assertEquals("abc\n", p2.getReturn().message().get(0).getData());

    // no new python process is created because it is isolated mode.
    assertEquals(pythonProcessNum, interpreterSetting.getAllInterpreterGroups().size());
  }

  @Test
  public void testRunNoteNonBlocking_Isolated() throws IOException, InterruptedException {
    LOG.debug("Running testRunNoteNonBlocking_Isolated");
    InterpreterSettingManager interpreterSettingManager =
            TestUtils.getInstance(InterpreterSettingManager.class);
    InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("python");
    int pythonProcessNum = interpreterSetting.getAllInterpreterGroups().size();

    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    from __future__ import print_function
    //    import time
    //    time.sleep(1)
    //    user='abc'
    // P2:
    //    %python
    //    print(user)
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python from __future__ import print_function\nimport time\ntime.sleep(1)\nuser='abc'");
    p2.setText("%python print(user)");

    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=false&isolated=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();

    // wait for all the paragraphs are done
    while(note1.isRunning()) {
      Thread.sleep(1000);
    }
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.FINISHED, p2.getStatus());
    assertEquals("abc\n", p2.getReturn().message().get(0).getData());

    // no new python process is created because it is isolated mode.
    assertEquals(pythonProcessNum, interpreterSetting.getAllInterpreterGroups().size());
  }

  @Test
  public void testRunNoteWithParams() throws IOException, InterruptedException {
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    name = z.input('name', 'world')
    //    print(name)
    // P2:
    //    %sh
    //    echo ${name|world}
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python name = z.input('name', 'world')\nprint(name)");
    p2.setText("%sh echo '${name=world}'");

    Map<String, Object> paramsMap = new HashMap<>();
    paramsMap.put("name", "zeppelin");
    ParametersRequest parametersRequest = new ParametersRequest(paramsMap);
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=false&isolated=true&",
            parametersRequest.toJson());
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK",resp.get("status"));
    post.close();

    // wait for all the paragraphs are done
    while(note1.isRunning()) {
      Thread.sleep(1000);
    }
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.FINISHED, p2.getStatus());
    assertEquals("zeppelin\n", p1.getReturn().message().get(0).getData());
    assertEquals("zeppelin\n", p2.getReturn().message().get(0).getData());

    // another attempt rest api call without params
    post = httpPost("/notebook/job/" + note1.getId() + "?blocking=false&isolated=true", "");
    assertThat(post, isAllowed());
    resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();

    // wait for all the paragraphs are done
    while(note1.isRunning()) {
      Thread.sleep(1000);
    }
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.FINISHED, p2.getStatus());
    assertEquals("world\n", p1.getReturn().message().get(0).getData());
    assertEquals("world\n", p2.getReturn().message().get(0).getData());
  }

  @Test
  public void testRunAllParagraph_FirstFailed() throws IOException {
    LOG.debug("Running testRunAllParagraph_FirstFailed");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    from __future__ import print_function
    //    import time
    //    time.sleep(1)
    //    print(user2)
    //
    // P2:
    //    %python
    //    user2='abc'
    //    print(user2)
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python from __future__ import print_function\nimport time\ntime.sleep(1)\nprint(user2)");
    p2.setText("%python user2='abc'\nprint(user2)");

    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post, isAllowed());

    assertEquals(Job.Status.ERROR, p1.getStatus());
    // p2 will be skipped because p1 is failed.
    assertEquals(Job.Status.READY, p2.getStatus());
    post.close();
  }

  @Test
  public void testCloneNote() throws IOException {
    LOG.debug("Running testCloneNote");
    String clonedNoteId = null;
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    CloseableHttpResponse post = httpPost("/notebook/" + note1.getId(), "");
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.debug("testCloneNote response\n" + postResponse);
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(postResponse,
            new TypeToken<Map<String, Object>>() {}.getType());
    clonedNoteId = (String) resp.get("body");
    post.close();

    CloseableHttpResponse get = httpGet("/notebook/" + clonedNoteId);
    assertThat(get, isAllowed());
    Map<String, Object> resp2 = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Object> resp2Body = (Map<String, Object>) resp2.get("body");

    //    assertEquals(resp2Body.get("name"), "Note " + clonedNoteId);
    get.close();
  }

  @Test
  public void testRenameNote() throws IOException {
    LOG.debug("Running testRenameNote");
    String oldName = "old_name";
    Note note = TestUtils.getInstance(LegacyNotebook.class).createNote(oldName, anonymous);
    assertEquals(oldName, note.getName());
    String noteId = note.getId();

    final String newName = "testName";
    String jsonRequest = "{\"name\": " + newName + "}";

    CloseableHttpResponse put = httpPut("/notebook/" + noteId + "/rename/", jsonRequest);
    assertThat("test testRenameNote:", put, isAllowed());
    put.close();

    assertEquals(newName, note.getName());
  }

  @Test
  public void testUpdateParagraphConfig() throws IOException {
    LOG.debug("Running testUpdateParagraphConfig");
    Note note = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    String noteId = note.getId();
    LegacyParagraph p = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    assertNull(p.getConfig().get("colWidth"));
    String paragraphId = p.getId();
    String jsonRequest = "{\"colWidth\": 6.0}";

    CloseableHttpResponse put = httpPut("/notebook/" + noteId + "/paragraph/" + paragraphId + "/config",
            jsonRequest);
    assertThat("test testUpdateParagraphConfig:", put, isAllowed());

    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(put.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Object> respBody = (Map<String, Object>) resp.get("body");
    Map<String, Object> config = (Map<String, Object>) respBody.get("config");
    put.close();

    assertEquals(6.0, config.get("colWidth"));
    note = TestUtils.getInstance(LegacyNotebook.class).getNote(noteId);
    assertEquals(6.0, note.getParagraph(paragraphId).getConfig().get("colWidth"));
  }

  @Test
  public void testClearAllParagraphOutput() throws IOException {
    LOG.debug("Running testClearAllParagraphOutput");
    Note note = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    LegacyParagraph p1 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    InterpreterResult result = new InterpreterResult(InterpreterResult.Code.SUCCESS,
            InterpreterResult.Type.TEXT, "result");
    p1.setResult(result);

    LegacyParagraph p2 = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p2.setReturn(result, new Throwable());

    // clear paragraph result
    CloseableHttpResponse put = httpPut("/notebook/" + note.getId() + "/clear", "");
    LOG.debug("test clear paragraph output response\n" + EntityUtils.toString(put.getEntity(), StandardCharsets.UTF_8));
    assertThat(put, isAllowed());
    put.close();

    // check if paragraph results are cleared
    CloseableHttpResponse get = httpGet("/notebook/" + note.getId() + "/paragraph/" + p1.getId());
    assertThat(get, isAllowed());
    Map<String, Object> resp1 = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Object> resp1Body = (Map<String, Object>) resp1.get("body");
    assertNull(resp1Body.get("result"));
    get.close();

    get = httpGet("/notebook/" + note.getId() + "/paragraph/" + p2.getId());
    assertThat(get, isAllowed());
    Map<String, Object> resp2 = gson.fromJson(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    Map<String, Object> resp2Body = (Map<String, Object>) resp2.get("body");
    assertNull(resp2Body.get("result"));
    get.close();
  }

  @Test
  public void testRunWithServerRestart() throws Exception {
    LOG.debug("Running testRunWithServerRestart");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note1", anonymous);
    // 2 paragraphs
    // P1:
    //    %python
    //    from __future__ import print_function
    //    import time
    //    time.sleep(1)
    //    user='abc'
    // P2:
    //    %python
    //    print(user)
    //
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    LegacyParagraph p2 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python from __future__ import print_function\nimport time\ntime.sleep(1)\nuser='abc'");
    p2.setText("%python print(user)");

    CloseableHttpResponse post1 = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post1, isAllowed());
    post1.close();
    CloseableHttpResponse put = httpPut("/notebook/" + note1.getId() + "/clear", "");
    LOG.debug("test clear paragraph output response\n" + EntityUtils.toString(put.getEntity(), StandardCharsets.UTF_8));
    assertThat(put, isAllowed());
    put.close();

    // restart server (while keeping interpreter configuration)
    AbstractTestRestApi.shutDown(false);
    startUp(LegacyNotebookRestApiTest.class.getSimpleName(), false);

    note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(note1.getId());
    p1 = note1.getParagraph(p1.getId());
    p2 = note1.getParagraph(p2.getId());

    CloseableHttpResponse post2 = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post2, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post2.getEntity(), StandardCharsets.UTF_8),
        new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post2.close();

    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(p2.getReturn().toString(), Job.Status.FINISHED, p2.getStatus());
    assertNotNull(p2.getReturn());
    assertEquals("abc\n", p2.getReturn().message().get(0).getData());
  }
}
