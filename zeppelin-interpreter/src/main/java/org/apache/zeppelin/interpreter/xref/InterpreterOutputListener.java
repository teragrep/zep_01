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
package org.apache.zeppelin.interpreter.xref;

/**
 * Listen InterpreterOutput buffer flush
 */
public interface InterpreterOutputListener {
  /**
   * update all message outputs
   */
  void onUpdateAll(InterpreterOutput out);

  /**
   * called when newline is detected
   * @param index
   * @param out
   * @param line
   */
  void onAppend(int index, InterpreterResultMessageOutput out, byte[] line);

  /**
   * when entire output is updated. eg) after detecting new display system
   * @param index
   * @param out
   */
  void onUpdate(int index, InterpreterResultMessageOutput out);
}
