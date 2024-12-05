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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.server.ZeppelinServer;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Zeppelin interpreter rest api tests.
 */
@Ignore(value="Flaky test, shuts down JVM")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InterpreterRestApiTest extends AbstractTestRestApi {
  private Gson gson = new Gson();
  private AuthenticationInfo anonymous;

  @BeforeClass
  public static void init() throws Exception {
    AbstractTestRestApi.startUp(InterpreterRestApiTest.class.getSimpleName());
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
  public void getAvailableInterpreters() throws IOException {
    // when
    CloseableHttpResponse get = httpGet("/interpreter");
    JsonObject body = getBodyFieldFromResponse(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8));

    // then
    assertThat(get, isAllowed());
    assertEquals(TestUtils.getInstance(Notebook.class).getInterpreterSettingManager()
                    .getInterpreterSettingTemplates().size(), body.entrySet().size());
    get.close();
  }

  @Test
  public void getSettings() throws IOException {
    // when
    CloseableHttpResponse get = httpGet("/interpreter/setting");
    // then
    assertThat(get, isAllowed());
    // DO NOT REMOVE: implies that body is properly parsed as an array
    JsonArray body = getArrayBodyFieldFromResponse(EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8));
    assertNotNull(body);
    get.close();
  }

  @Test
  public void testGetNonExistInterpreterSetting() throws IOException {
    // when
    String nonExistInterpreterSettingId = "apache_.zeppelin_1s_.aw3some$";
    CloseableHttpResponse get = httpGet("/interpreter/setting/" + nonExistInterpreterSettingId);

    // then
    assertThat("Test get method:", get, isNotFound());
    get.close();
  }

  @Test
  public void testSettingsCRUD() throws IOException {
    // when: call create setting API
    String rawRequest = "{\"name\":\"md3\",\"group\":\"md\"," +
            "\"properties\":{\"propname\": {\"value\": \"propvalue\", \"name\": \"propname\", " +
            "\"type\": \"textarea\"}}," +
            "\"interpreterGroup\":[{\"class\":\"org.apache.zeppelin.markdown.Markdown\"," +
            "\"name\":\"md\"}],\"dependencies\":[]," +
            "\"option\": { \"remote\": true, \"session\": false }}";
    JsonObject jsonRequest = gson.fromJson(rawRequest, JsonElement.class).getAsJsonObject();
    CloseableHttpResponse post = httpPost("/interpreter/setting/", jsonRequest.toString());
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.info("testSettingCRUD create response\n" + postResponse);
    InterpreterSetting created = convertResponseToInterpreterSetting(postResponse);
    String newSettingId = created.getId();
    // then : call create setting API
    assertThat("test create method:", post, isAllowed());
    post.close();

    // when: call read setting API
    CloseableHttpResponse get = httpGet("/interpreter/setting/" + newSettingId);
    String getResponse = EntityUtils.toString(get.getEntity(), StandardCharsets.UTF_8);
    LOG.info("testSettingCRUD get response\n" + getResponse);
    InterpreterSetting previouslyCreated = convertResponseToInterpreterSetting(getResponse);
    // then : read Setting API
    assertThat("Test get method:", get, isAllowed());
    assertEquals(newSettingId, previouslyCreated.getId());
    get.close();

    // when: call update setting API
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", "propname2");
    jsonObject.addProperty("value", "this is new prop");
    jsonObject.addProperty("type", "textarea");
    jsonRequest.getAsJsonObject("properties").add("propname2", jsonObject);
    CloseableHttpResponse put = httpPut("/interpreter/setting/" + newSettingId, jsonRequest.toString());
    LOG.info("testSettingCRUD update response\n" + EntityUtils.toString(put.getEntity(), StandardCharsets.UTF_8));
    // then: call update setting API
    assertThat("test update method:", put, isAllowed());
    put.close();

    // when: call delete setting API
    CloseableHttpResponse delete = httpDelete("/interpreter/setting/" + newSettingId);
    LOG.info("testSettingCRUD delete response\n" +  EntityUtils.toString(delete.getEntity(), StandardCharsets.UTF_8));
    // then: call delete setting API
    assertThat("Test delete method:", delete, isAllowed());
    delete.close();
  }

  @Test
  public void testSettingsCreateWithEmptyJson() throws IOException {
    // Call Create Setting REST API
    CloseableHttpResponse post = httpPost("/interpreter/setting/", "");
    LOG.info("testSettingCRUD create response\n" + EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8));
    assertThat("test create method:", post, isBadRequest());
    post.close();
  }

  @Test
  public void testSettingsCreateWithInvalidName() throws IOException {
    String reqBody = "{"
        + "\"name\": \"mdName\","
        + "\"group\": \"md\","
        + "\"properties\": {"
        + "\"propname\": {"
        + "\"value\": \"propvalue\","
        + "\"name\": \"propname\","
        + "\"type\": \"textarea\""
        + "}"
        + "},"
        + "\"interpreterGroup\": ["
        + "{"
        + "\"class\": \"org.apache.zeppelin.markdown.Markdown\","
        + "\"name\": \"md\""
        + "}"
        + "],"
        + "\"dependencies\": [],"
        + "\"option\": {"
        + "\"remote\": true,"
        + "\"session\": false"
        + "}"
        + "}";
    JsonObject jsonRequest = gson.fromJson(StringUtils.replace(reqBody, "mdName", "mdValidName"), JsonElement.class).getAsJsonObject();
    CloseableHttpResponse post = httpPost("/interpreter/setting/", jsonRequest.toString());
    String postResponse = EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8);
    LOG.info("testSetting with valid name\n" + postResponse);
    InterpreterSetting created = convertResponseToInterpreterSetting(postResponse);
    String newSettingId = created.getId();
    // then : call create setting API
    assertThat("test create method:", post, isAllowed());
    post.close();

    // when: call delete setting API
    CloseableHttpResponse delete = httpDelete("/interpreter/setting/" + newSettingId);
    LOG.info("testSetting delete response\n" + EntityUtils.toString(delete.getEntity(), StandardCharsets.UTF_8));
    // then: call delete setting API
    assertThat("Test delete method:", delete, isAllowed());
    delete.close();


    JsonObject jsonRequest2 = gson.fromJson(StringUtils.replace(reqBody, "mdName", "name space"), JsonElement.class).getAsJsonObject();
    CloseableHttpResponse post2 = httpPost("/interpreter/setting/", jsonRequest2.toString());
    LOG.info("testSetting with name with space\n" + EntityUtils.toString(post2.getEntity(), StandardCharsets.UTF_8));
    assertThat("test create method with space:", post2, isNotFound());
    post2.close();

    JsonObject jsonRequest3 = gson.fromJson(StringUtils.replace(reqBody, "mdName", ""), JsonElement.class).getAsJsonObject();
    CloseableHttpResponse post3 = httpPost("/interpreter/setting/", jsonRequest3.toString());
    LOG.info("testSetting with empty name\n" + EntityUtils.toString(post3.getEntity(), StandardCharsets.UTF_8));
    assertThat("test create method with empty name:", post3, isNotFound());
    post3.close();

  }

  public void testInterpreterRestart() throws IOException, InterruptedException {
    Note note = null;
    try {
      // when: create new note
      note = TestUtils.getInstance(Notebook.class).createNote("note1", anonymous);
      note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
      Paragraph p = note.getLastParagraph();
      Map config = p.getConfig();
      config.put("enabled", true);

      // when: run markdown paragraph
      p.setConfig(config);
      p.setText("%md markdown");
      p.setAuthenticationInfo(anonymous);
      note.run(p.getId());
      while (p.getStatus() != Status.FINISHED) {
        Thread.sleep(100);
      }
      assertEquals(p.getReturn().message().get(0).getData(), getSimulatedMarkdownResult("markdown"));

      // when: restart interpreter
      for (InterpreterSetting setting : note.getBindedInterpreterSettings(new ArrayList<>())) {
        if (setting.getName().equals("md")) {
          // call restart interpreter API
          CloseableHttpResponse put = httpPut("/interpreter/setting/restart/" + setting.getId(), "");
          assertThat("test interpreter restart:", put, isAllowed());
          put.close();
          break;
        }
      }

      // when: run markdown paragraph, again
      p = note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
      p.setConfig(config);
      p.setText("%md markdown restarted");
      p.setAuthenticationInfo(anonymous);
      note.run(p.getId());
      while (p.getStatus() != Status.FINISHED) {
        Thread.sleep(100);
      }

      // then
      assertEquals(p.getReturn().message().get(0).getData(),
              getSimulatedMarkdownResult("markdown restarted"));
    } finally {
      if (null != note) {
        TestUtils.getInstance(Notebook.class).removeNote(note, anonymous);
      }
    }
  }

  @Test
  public void testRestartInterpreterPerNote() throws IOException, InterruptedException {
    Note note = null;
    try {
      // when: create new note
      note = TestUtils.getInstance(Notebook.class).createNote("note2", anonymous);
      note.addNewParagraph(AuthenticationInfo.ANONYMOUS);
      Paragraph p = note.getLastParagraph();
      Map config = p.getConfig();
      config.put("enabled", true);

      // when: run markdown paragraph.
      p.setConfig(config);
      p.setText("%md markdown");
      p.setAuthenticationInfo(anonymous);
      note.run(p.getId());
      while (p.getStatus() != Status.FINISHED) {
        Thread.sleep(100);
      }
      assertEquals(p.getReturn().message().get(0).getData(), getSimulatedMarkdownResult("markdown"));

      // when: get md interpreter
      InterpreterSetting mdIntpSetting = null;
      for (InterpreterSetting setting : note.getBindedInterpreterSettings(new ArrayList<>())) {
        if (setting.getName().equals("md")) {
          mdIntpSetting = setting;
          break;
        }
      }

      String jsonRequest = "{\"noteId\":\"" + note.getId() + "\"}";

      // Restart isolated mode of Interpreter for note.
      mdIntpSetting.getOption().setPerNote(InterpreterOption.ISOLATED);
      CloseableHttpResponse put = httpPut("/interpreter/setting/restart/" + mdIntpSetting.getId(), jsonRequest);
      assertThat("isolated interpreter restart:", put, isAllowed());
      put.close();

      // Restart scoped mode of Interpreter for note.
      mdIntpSetting.getOption().setPerNote(InterpreterOption.SCOPED);
      put = httpPut("/interpreter/setting/restart/" + mdIntpSetting.getId(), jsonRequest);
      assertThat("scoped interpreter restart:", put, isAllowed());
      put.close();

      // Restart shared mode of Interpreter for note.
      mdIntpSetting.getOption().setPerNote(InterpreterOption.SHARED);
      put = httpPut("/interpreter/setting/restart/" + mdIntpSetting.getId(), jsonRequest);
      assertThat("shared interpreter restart:", put, isAllowed());
      put.close();

    } finally {
      if (null != note) {
        TestUtils.getInstance(Notebook.class).removeNote(note, anonymous);
      }
    }
  }

  @Test
  public void testListRepository() throws IOException {
    CloseableHttpResponse get = httpGet("/interpreter/repository");
    assertThat(get, isAllowed());
    get.close();
  }

  @Test
  public void testAddDeleteRepository() throws IOException {
    // Call create repository API
    String repoId = "securecentral";
    String jsonRequest = "{\"id\":\"" + repoId +
        "\",\"url\":\"https://repo1.maven.org/maven2\",\"snapshot\":\"false\"}";

    CloseableHttpResponse post = httpPost("/interpreter/repository/", jsonRequest);
    assertThat("Test create method:", post, isAllowed());
    post.close();

    // Call delete repository API
    CloseableHttpResponse delete = httpDelete("/interpreter/repository/" + repoId);
    assertThat("Test delete method:", delete, isAllowed());
    delete.close();
  }

  private JsonObject getBodyFieldFromResponse(String rawResponse) {
    JsonObject response = gson.fromJson(rawResponse, JsonElement.class).getAsJsonObject();
    return response.getAsJsonObject("body");
  }

  private JsonArray getArrayBodyFieldFromResponse(String rawResponse) {
    JsonObject response = gson.fromJson(rawResponse, JsonElement.class).getAsJsonObject();
    return response.getAsJsonArray("body");
  }

  private InterpreterSetting convertResponseToInterpreterSetting(String rawResponse) {
    return gson.fromJson(getBodyFieldFromResponse(rawResponse), InterpreterSetting.class);
  }

  private static String getSimulatedMarkdownResult(String markdown) {
    return String.format("<div class=\"markdown-body\">\n<p>%s</p>\n\n</div>", markdown);
  }
}
