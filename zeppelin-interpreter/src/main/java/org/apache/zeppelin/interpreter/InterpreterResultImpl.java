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

import com.google.gson.Gson;
import org.apache.zeppelin.interpreter.xref.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Interpreter result template.
 */
public class InterpreterResultImpl implements InterpreterResult {
  transient Logger logger = LoggerFactory.getLogger(InterpreterResultImpl.class);
  private static final Gson gson = new Gson();

  Code code;
  List<InterpreterResultMessage> msg = new LinkedList<>();

  public InterpreterResultImpl(Code code) {
    this.code = code;
  }

  public InterpreterResultImpl(Code code, List<InterpreterResultMessage> msgs) {
    this.code = code;
    msg.addAll(msgs);
  }

  public InterpreterResultImpl(Code code, String msg) {
    this.code = code;
    add(msg);
  }

  public InterpreterResultImpl(Code code, Type type, String msg) {
    this.code = code;
    add(type, msg);
  }

  /**
   * Automatically detect %[display_system] directives
   * @param msg
   */
  @Override
  public void add(String msg) {
    InterpreterOutput out = new InterpreterOutputImpl();
    try {
      out.write(msg);
      out.flush();
      this.msg.addAll(out.toInterpreterResultMessage());
      out.close();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }

  }

  @Override
  public void add(Type type, String data) {
    msg.add(new InterpreterResultMessageImpl(type, data));
  }

  @Override
  public void add(InterpreterResultMessage interpreterResultMessage) {
    msg.add(interpreterResultMessage);
  }

  @Override
  public Code code() {
    return code;
  }

  @Override
  public List<InterpreterResultMessage> message() {
    return msg;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static InterpreterResult fromJson(String json) {
    return gson.fromJson(json, InterpreterResultImpl.class);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Type prevType = null;
    for (InterpreterResultMessage m : msg) {
      if (prevType != null) {
        sb.append("\n");
        if (prevType == Type.TABLE) {
          sb.append("\n");
        }
      }
      sb.append(m.toString());
      prevType = m.getType();
    }

    return sb.toString();
  }
}
