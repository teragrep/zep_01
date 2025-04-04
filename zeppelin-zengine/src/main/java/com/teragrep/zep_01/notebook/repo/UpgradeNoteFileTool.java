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

package com.teragrep.zep_01.notebook.repo;

import org.apache.commons.cli.*;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;

import java.io.IOException;

public class UpgradeNoteFileTool {

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    Option input = new Option("d", "deleteOld", false, "Whether delete old note file");
    options.addOption(input);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e);
      System.exit(1);
    }

    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    NotebookRepoSync notebookRepoSync = new NotebookRepoSync(conf);
    notebookRepoSync.convertNoteFiles(conf, cmd.hasOption("d"));
  }
}
