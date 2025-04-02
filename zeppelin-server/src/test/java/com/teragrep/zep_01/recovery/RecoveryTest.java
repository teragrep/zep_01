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
package com.teragrep.zep_01.recovery;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.InterpreterSettingManager;
import com.teragrep.zep_01.interpreter.ManagedInterpreterGroup;
import com.teragrep.zep_01.interpreter.recovery.FileSystemRecoveryStorage;
import com.teragrep.zep_01.interpreter.recovery.StopInterpreter;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.LegacyNotebook;
import com.teragrep.zep_01.notebook.LegacyParagraph;
import com.teragrep.zep_01.rest.AbstractTestRestApi;
import com.teragrep.zep_01.scheduler.Job;
import com.teragrep.zep_01.server.ZeppelinServer;
import com.teragrep.zep_01.user.AuthenticationInfo;
import com.teragrep.zep_01.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Ignore(value="Tests will fail at notebook.createNote with a 'IndexOutOfBounds Index 0 out of bounds for length 0' message, might be relics of test-to-test contamination")
public class RecoveryTest extends AbstractTestRestApi {

  private Gson gson = new Gson();
  private static File recoveryDir = null;

  private LegacyNotebook notebook;

  private AuthenticationInfo anonymous = new AuthenticationInfo("anonymous");

  @Before
  public void init() throws Exception {
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_STORAGE_CLASS.getVarName(),
            FileSystemRecoveryStorage.class.getName());
    recoveryDir = new File("target/recovery").toPath().toAbsolutePath().toFile();
    recoveryDir.mkdirs();
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_DIR.getVarName(),
            recoveryDir.getAbsolutePath());
    startUp(RecoveryTest.class.getSimpleName());

    notebook = ZeppelinServer.sharedServiceLocator.getService(LegacyNotebook.class);
  }

  @After
  public void destroy() throws Exception {
    shutDown(true, true);
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_STORAGE_CLASS.getVarName(),
            ZeppelinConfiguration.ConfVars.ZEPPELIN_RECOVERY_STORAGE_CLASS.getStringValue());
  }

  @Test
  public void testRecovery() throws Exception {
    LOG.debug("Test testRecovery");
    Note note1 = notebook.createNote("note1", anonymous);

    // run python interpreter and create new variable `user`
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python user='abc'");
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() +"?blocking=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    TestUtils.getInstance(LegacyNotebook.class).saveNote(note1, anonymous);

    // shutdown zeppelin and restart it
    shutDown();
    startUp(RecoveryTest.class.getSimpleName(), false);

    // run the paragraph again, but change the text to print variable `user`
    note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(note1.getId());
    Thread.sleep(10 * 1000);
    note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(note1.getId());
    p1 = note1.getParagraph(p1.getId());
    p1.setText("%python print(user)");
    post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals("abc\n", p1.getReturn().message().get(0).getData());
  }

  @Test
  public void testRecovery_2() throws Exception {
    LOG.debug("Test testRecovery_2");
    Note note1 = notebook.createNote("note2", AuthenticationInfo.ANONYMOUS);

    // run python interpreter and create new variable `user`
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python user='abc'");
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    TestUtils.getInstance(LegacyNotebook.class).saveNote(note1, AuthenticationInfo.ANONYMOUS);
    // restart the python interpreter
    TestUtils.getInstance(LegacyNotebook.class).getInterpreterSettingManager().restart(
        ((ManagedInterpreterGroup) p1.getBindedInterpreter().getInterpreterGroup())
            .getInterpreterSetting().getId()
    );

    // shutdown zeppelin and restart it
    shutDown();
    startUp(RecoveryTest.class.getSimpleName(), false);

    Thread.sleep(5 * 1000);
    // run the paragraph again, but change the text to print variable `user`.
    // can not recover the python interpreter, because it has been shutdown.
    note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(note1.getId());
    p1 = note1.getParagraph(p1.getId());
    p1.setText("%python print(user)");
    post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.ERROR, p1.getStatus());
  }

  @Test
  public void testRecovery_3() throws Exception {
    LOG.debug("Test testRecovery_3");
    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note3", AuthenticationInfo.ANONYMOUS);

    // run python interpreter and create new variable `user`
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python user='abc'");
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertThat(post, isAllowed());
    Map<String, Object> resp = gson.fromJson(EntityUtils.toString(post.getEntity(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    TestUtils.getInstance(LegacyNotebook.class).saveNote(note1, AuthenticationInfo.ANONYMOUS);

    // shutdown zeppelin and restart it
    shutDown();
    StopInterpreter.main(new String[]{});

    startUp(RecoveryTest.class.getSimpleName(), false);

    Thread.sleep(5 * 1000);
    // run the paragraph again, but change the text to print variable `user`.
    // can not recover the python interpreter, because it has been shutdown.
    note1 = TestUtils.getInstance(LegacyNotebook.class).getNote(note1.getId());
    p1 = note1.getParagraph(p1.getId());
    p1.setText("%python print(user)");
    post = httpPost("/notebook/job/" + note1.getId() + "?blocking=true", "");
    assertEquals("OK", resp.get("status"));
    post.close();
    assertEquals(Job.Status.ERROR, p1.getStatus());
  }

  @Test
  public void testRecovery_Finished_Paragraph_python() throws Exception {
    LOG.debug("Test testRecovery_Finished_Paragraph_python");
    InterpreterSettingManager interpreterSettingManager = TestUtils.getInstance(InterpreterSettingManager.class);
    InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("python");
    interpreterSetting.setProperty("zeppelin.python.useIPython", "false");
    interpreterSetting.setProperty("zeppelin.interpreter.result.cache", "100");

    Note note1 = TestUtils.getInstance(LegacyNotebook.class).createNote("note4", AuthenticationInfo.ANONYMOUS);

    // run  paragraph async, print 'hello' after 10 seconds
    LegacyParagraph p1 = note1.addNewParagraph(AuthenticationInfo.ANONYMOUS);
    p1.setText("%python import time\n" +
            "for i in range(1, 10):\n" +
            "    time.sleep(1)\n" +
            "    print(i)");
    CloseableHttpResponse post = httpPost("/notebook/job/" + note1.getId() + "/" + p1.getId(), "");
    assertThat(post, isAllowed());
    post.close();

    // wait until paragraph is running
    while(p1.getStatus() != Job.Status.RUNNING) {
      Thread.sleep(1000);
    }

    // shutdown zeppelin and restart it
    shutDown();
    // sleep 15 seconds to make sure the paragraph is finished
    Thread.sleep(15 * 1000);

    startUp(RecoveryTest.class.getSimpleName(), false);
    // sleep 10 seconds to make sure recovering is finished
    Thread.sleep(10 * 1000);

    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals("1\n" +
            "2\n" +
            "3\n" +
            "4\n" +
            "5\n" +
            "6\n" +
            "7\n" +
            "8\n" +
            "9\n", p1.getReturn().message().get(0).getData());
  }
}
