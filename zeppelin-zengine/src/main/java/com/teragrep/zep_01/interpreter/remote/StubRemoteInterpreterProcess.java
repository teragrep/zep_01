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

package com.teragrep.zep_01.interpreter.remote;

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.YarnAppMonitor;
import com.teragrep.zep_01.interpreter.util.ProcessLauncher;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StubRemoteInterpreterProcess extends RemoteInterpreterProcess {

  public StubRemoteInterpreterProcess() {
    super(0, 0, "", -1);
  }

  @Override
  public String getInterpreterGroupId() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public String getInterpreterSettingName() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public void start(String userName) throws IOException {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public void stop() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public String getHost() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public int getPort() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public boolean isRunning() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public void processStarted(int port, String host) {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public String getErrorMessage() {
    throw new RuntimeException("InterpreterProcess is a stub!");
  }

  @Override
  public boolean isStub() {
    return true;
  }
}
