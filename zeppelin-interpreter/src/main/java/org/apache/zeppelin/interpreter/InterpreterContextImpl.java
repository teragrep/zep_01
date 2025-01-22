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

package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.display.GUIImpl;
import com.teragrep.zep_04.display.AngularObjectRegistry;
import com.teragrep.zep_04.display.GUI;
import com.teragrep.zep_04.remote.RemoteInterpreterEventClient;
import com.teragrep.zep_04.interpreter.InterpreterContext;
import com.teragrep.zep_04.interpreter.InterpreterOutput;
import com.teragrep.zep_04.resource.ResourcePool;
import com.teragrep.zep_04.user.AuthenticationInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Interpreter context
 */
public class InterpreterContextImpl implements InterpreterContext {

  public InterpreterOutput out;

  private String noteId;
  private String noteName;
  private String replName;
  private String paragraphTitle;
  private String paragraphId;
  private String paragraphText;
  private AuthenticationInfo authenticationInfo;
  private Map<String, Object> config = new HashMap<>();
  private GUI gui = new GUIImpl();
  private GUI noteGui = new GUIImpl();
  private AngularObjectRegistry angularObjectRegistry;
  private ResourcePool resourcePool;
  private String interpreterClassName;
  private Map<String, Integer> progressMap;
  private Map<String, String> localProperties = new HashMap<>();
  private RemoteInterpreterEventClient intpEventClient;

  /**
   * Builder class for InterpreterContext
   */
  public static class Builder {
    private InterpreterContextImpl context;

    public Builder() {
      context = new InterpreterContextImpl();
    }

    public Builder setNoteId(String noteId) {
      context.noteId = noteId;
      return this;
    }

    public Builder setNoteName(String noteName) {
      context.noteName = noteName;
      return this;
    }

    public Builder setParagraphId(String paragraphId) {
      context.paragraphId = paragraphId;
      return this;
    }

    public Builder setInterpreterClassName(String intpClassName) {
      context.interpreterClassName = intpClassName;
      return this;
    }

    public Builder setAngularObjectRegistry(AngularObjectRegistry angularObjectRegistry) {
      context.angularObjectRegistry = angularObjectRegistry;
      return this;
    }

    public Builder setResourcePool(ResourcePool resourcePool) {
      context.resourcePool = resourcePool;
      return this;
    }

    public Builder setReplName(String replName) {
      context.replName = replName;
      return this;
    }

    public Builder setAuthenticationInfo(AuthenticationInfo authenticationInfo) {
      context.authenticationInfo = authenticationInfo;
      return this;
    }

    public Builder setConfig(Map<String, Object> config) {
      if (config != null) {
        context.config = new HashMap<>(config);
      }
      return this;
    }

    public Builder setGUI(GUI gui) {
      context.gui = gui;
      return this;
    }

    public Builder setNoteGUI(GUI noteGUI) {
      context.noteGui = noteGUI;
      return this;
    }

    public Builder setInterpreterOut(InterpreterOutput out) {
      context.out = out;
      return this;
    }

    public Builder setIntpEventClient(RemoteInterpreterEventClient intpEventClient) {
      context.intpEventClient = intpEventClient;
      return this;
    }

    public Builder setProgressMap(Map<String, Integer> progressMap) {
      context.progressMap = progressMap;
      return this;
    }

    public Builder setParagraphText(String paragraphText) {
      context.paragraphText = paragraphText;
      return this;
    }

    public Builder setParagraphTitle(String paragraphTitle) {
      context.paragraphTitle = paragraphTitle;
      return this;
    }

    public Builder setLocalProperties(Map<String, String> localProperties) {
      context.localProperties = localProperties;
      return this;
    }

    public InterpreterContextImpl build() {
      return context;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private InterpreterContextImpl() {

  }


  @Override
  public String getNoteId() {
    return noteId;
  }

  @Override
  public String getNoteName() {
    return noteName;
  }

  @Override
  public String getReplName() {
    return replName;
  }

  @Override
  public String getParagraphId() {
    return paragraphId;
  }

  @Override
  public void setParagraphId(String paragraphId) {
    this.paragraphId = paragraphId;
  }

  @Override
  public String getParagraphText() {
    return paragraphText;
  }

  @Override
  public String getParagraphTitle() {
    return paragraphTitle;
  }

  @Override
  public Map<String, String> getLocalProperties() {
    return localProperties;
  }

  @Override
  public String getStringLocalProperty(String key, String defaultValue) {
    return localProperties.getOrDefault(key, defaultValue);
  }

  @Override
  public int getIntLocalProperty(String key, int defaultValue) {
    return Integer.parseInt(localProperties.getOrDefault(key, defaultValue + ""));
  }

  @Override
  public long getLongLocalProperty(String key, int defaultValue) {
    return Long.parseLong(localProperties.getOrDefault(key, defaultValue + ""));
  }

  @Override
  public double getDoubleLocalProperty(String key, double defaultValue) {
    return Double.parseDouble(localProperties.getOrDefault(key, defaultValue + ""));
  }

  @Override
  public boolean getBooleanLocalProperty(String key, boolean defaultValue) {
    return Boolean.parseBoolean(localProperties.getOrDefault(key, defaultValue + ""));
  }

  @Override
  public AuthenticationInfo getAuthenticationInfo() {
    return authenticationInfo;
  }

  @Override
  public Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public GUI getGui() {
    return gui;
  }

  @Override
  public GUI getNoteGui() {
    return noteGui;
  }

  @Override
  public AngularObjectRegistry getAngularObjectRegistry() {
    return angularObjectRegistry;
  }

  @Override
  public void setAngularObjectRegistry(AngularObjectRegistry angularObjectRegistry) {
    this.angularObjectRegistry = angularObjectRegistry;
  }

  @Override
  public ResourcePool getResourcePool() {
    return resourcePool;
  }

  @Override
  public void setResourcePool(ResourcePool resourcePool) {
    this.resourcePool = resourcePool;
  }

  @Override
  public String getInterpreterClassName() {
    return interpreterClassName;
  }

  @Override
  public void setInterpreterClassName(String className) {
    this.interpreterClassName = className;
  }

  @Override
  public RemoteInterpreterEventClient getIntpEventClient() {
    return intpEventClient;
  }

  @Override
  public void setIntpEventClient(RemoteInterpreterEventClient intpEventClient) {
    this.intpEventClient = intpEventClient;
  }

  @Override
  public InterpreterOutput out() {
    return out;
  }

  /**
   * Set progress of paragraph manually
   * @param n integer from 0 to 100
   */
  @Override
  public void setProgress(int n) {
    if (progressMap != null) {
      n = Math.max(n, 0);
      n = Math.min(n, 100);
      progressMap.put(paragraphId, new Integer(n));
    }
  }
}
