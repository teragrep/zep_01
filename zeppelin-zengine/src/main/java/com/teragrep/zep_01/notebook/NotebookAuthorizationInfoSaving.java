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

package com.teragrep.zep_01.notebook;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.teragrep.zep_01.common.JsonSerializable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Only used for saving NotebookAuthorization info
 */
public class NotebookAuthorizationInfoSaving implements JsonSerializable {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  public Map<String, Map<String, Set<String>>> authInfo;

  public NotebookAuthorizationInfoSaving(Map<String, NoteAuth> notesAuth) {
    this.authInfo = new HashMap<>();
    for (Map.Entry<String, NoteAuth> entry : notesAuth.entrySet()) {
      this.authInfo.put(entry.getKey(), entry.getValue().toMap());
    }
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static NotebookAuthorizationInfoSaving fromJson(String json) {
    return gson.fromJson(json, NotebookAuthorizationInfoSaving.class);
  }
}
