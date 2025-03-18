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

package com.teragrep.zep_01.plugin;

import com.teragrep.zep_01.notebook.repo.GitNotebookRepo;
import com.teragrep.zep_01.notebook.repo.NotebookRepo;
import com.teragrep.zep_01.notebook.repo.OldGitNotebookRepo;
import com.teragrep.zep_01.notebook.repo.OldNotebookRepo;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;


public class PluginManagerTest {

  @Test
  public void testLoadGitNotebookRepo() throws IOException {
    NotebookRepo notebookRepo = PluginManager.get()
            .loadNotebookRepo("com.teragrep.zep_01.notebook.repo.GitNotebookRepo");
    assertTrue(notebookRepo instanceof GitNotebookRepo);

    OldNotebookRepo oldNotebookRepo = PluginManager.get()
            .loadOldNotebookRepo("com.teragrep.zep_01.notebook.repo.GitNotebookRepo");
    assertTrue(oldNotebookRepo instanceof OldGitNotebookRepo);
  }
}
