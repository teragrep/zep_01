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

import com.teragrep.zep_04.interpreter.InterpreterResultMessage;
import com.teragrep.zep_04.interpreter.Type;

/**
 * Interpreter result message
 */
public class InterpreterResultMessageImpl implements InterpreterResultMessage {
  Type type;
  String data;

  public InterpreterResultMessageImpl(Type type, String data) {
    this.type = type;
    this.data = data;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String getData() {
    return data;
  }

  @Override
  public String toString() {
    return "%" + type.name().toLowerCase() + " " + data;
  }
}
