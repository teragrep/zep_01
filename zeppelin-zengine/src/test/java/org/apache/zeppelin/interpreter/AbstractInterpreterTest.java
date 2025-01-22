/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.xref.display.AngularObjectRegistryListener;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;

import static org.mockito.Mockito.mock;

/**
 * This class will load configuration files under
 *   src/test/resources/interpreter
 *   src/test/resources/conf
 *
 * to construct InterpreterSettingManager and InterpreterFactory properly
 *
 */
public abstract class AbstractInterpreterTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractInterpreterTest.class);

  protected InterpreterSettingManager interpreterSettingManager;
  protected InterpreterFactory interpreterFactory;
  protected Notebook mockNotebook;
  protected File zeppelinHome;
  protected File interpreterDir;
  protected File confDir;
  protected File notebookDir;
  protected ZeppelinConfiguration conf;

  @Before
  public void setUp() throws Exception {
    // copy the resources files to a temp folder
    zeppelinHome = new File("..");
    LOGGER.debug("ZEPPELIN_HOME: " + zeppelinHome.getAbsolutePath());
    interpreterDir = new File(zeppelinHome, "zeppelin-zengine/target/interpreter_" + getClass().getSimpleName() + Instant.now().toEpochMilli());
    confDir = new File(zeppelinHome, "zeppelin-zengine/target/conf_" + getClass().getSimpleName() + Instant.now().toEpochMilli());
    notebookDir = new File(zeppelinHome, "zeppelin-zengine/target/notebook_" + getClass().getSimpleName() + Instant.now().toEpochMilli());

    interpreterDir.mkdirs();
    confDir.mkdirs();
    notebookDir.mkdirs();

    FileUtils.copyDirectory(new File("src/test/resources/interpreter"), interpreterDir);
    FileUtils.copyDirectory(new File("src/test/resources/conf"), confDir);

    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME.getVarName(), zeppelinHome.getAbsolutePath());
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(), confDir.getAbsolutePath());
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_DIR.getVarName(), interpreterDir.getAbsolutePath());
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(), notebookDir.getAbsolutePath());
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT.getVarName(), "test");

    conf = ZeppelinConfiguration.create();
    interpreterSettingManager = new InterpreterSettingManager(conf,
        mock(AngularObjectRegistryListener.class), mock(RemoteInterpreterProcessListener.class));
    interpreterFactory = new InterpreterFactory(interpreterSettingManager);

    mockNotebook = mock(Notebook.class);
    interpreterSettingManager.setNotebook(mockNotebook);
  }

  @After
  public void tearDown() throws Exception {
    interpreterSettingManager.close();
  }

  protected Note createNote() {
    return new Note("test", "test", interpreterFactory, interpreterSettingManager, null, null, null);
  }

  protected InterpreterContextImpl createDummyInterpreterContext() {
    return InterpreterContextImpl.builder()
        .setNoteId("noteId")
        .setParagraphId("paragraphId")
        .build();
  }
}
