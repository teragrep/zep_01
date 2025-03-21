/**
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

namespace java com.teragrep.zep_01.interpreter.thrift

struct RemoteInterpreterContext {
  1: string noteId,
  2: string noteName,
  3: string paragraphId,
  4: string replName,
  5: string paragraphTitle,
  6: string paragraphText,
  7: string authenticationInfo,
  8: string config,   // json serialized config
  9: string gui,      // json serialized gui
  10: string noteGui,      // json serialized note gui
  11: map<string, string> localProperties
}

struct RemoteInterpreterResultMessage {
  1: string type,
  2: string data
}
struct RemoteInterpreterResult {
  1: string code,
  2: list<RemoteInterpreterResultMessage> msg,
  3: string config,   // json serialized config
  4: string gui       // json serialized gui
  5: string noteGui       // json serialized note gui
}

enum RemoteInterpreterEventType {
  NO_OP = 1,
  ANGULAR_OBJECT_ADD = 2,
  ANGULAR_OBJECT_UPDATE = 3,
  ANGULAR_OBJECT_REMOVE = 4,
  RUN_INTERPRETER_CONTEXT_RUNNER = 5,
  RESOURCE_POOL_GET_ALL = 6,
  RESOURCE_GET = 7
  OUTPUT_APPEND = 8,
  OUTPUT_UPDATE = 9,
  OUTPUT_UPDATE_ALL = 10,
  ANGULAR_REGISTRY_PUSH = 11,
  APP_STATUS_UPDATE = 12,
  META_INFOS = 13,
  REMOTE_ZEPPELIN_SERVER_RESOURCE = 14,
  RESOURCE_INVOKE_METHOD = 15,
  PARA_INFOS = 16
}


struct RemoteInterpreterEvent {
  1: RemoteInterpreterEventType type,
  2: string data      // json serialized data
}

struct RemoteApplicationResult {
  1: bool success,
  2: string msg
}

/*
 * The below variables(name, value) will be connected to getCompletions in paragraph.controller.js
 *
 * name: which is shown in the suggestion list
 * value: actual return value what you selected
 */
struct InterpreterCompletion {
  1: string name,
  2: string value,
  3: string meta
}

exception InterpreterRPCException {
  1: string errorMessage,
}

service RemoteInterpreterService {

  void createInterpreter(1: string intpGroupId, 2: string sessionId, 3: string className, 4: map<string, string> properties, 5: string userName) throws (1: InterpreterRPCException ex);
  void init(1: map<string, string> properties) throws (1: InterpreterRPCException ex);
  void open(1: string sessionId, 2: string className) throws (1: InterpreterRPCException ex);
  void close(1: string sessionId, 2: string className) throws (1: InterpreterRPCException ex);
  void reconnect(1: string host, 2: i32 port) throws (1: InterpreterRPCException ex);
  RemoteInterpreterResult interpret(1: string sessionId, 2: string className, 3: string st, 4: RemoteInterpreterContext interpreterContext) throws (1: InterpreterRPCException ex);
  void cancel(1: string sessionId, 2: string className, 3: RemoteInterpreterContext interpreterContext) throws (1: InterpreterRPCException ex);
  i32 getProgress(1: string sessionId, 2: string className, 3: RemoteInterpreterContext interpreterContext) throws (1: InterpreterRPCException ex);
  string getFormType(1: string sessionId, 2: string className) throws (1: InterpreterRPCException ex);
  list<InterpreterCompletion> completion(1: string sessionId, 2: string className, 3: string buf, 4: i32 cursor, 5: RemoteInterpreterContext interpreterContext) throws (1: InterpreterRPCException ex);
  void shutdown();

  string getStatus(1: string sessionId, 2:string jobId) throws (1: InterpreterRPCException ex);

  list<string> resourcePoolGetAll() throws (1: InterpreterRPCException ex);
  // get value of resource
  binary resourceGet(1: string sessionId, 2: string paragraphId, 3: string resourceName) throws (1: InterpreterRPCException ex);
  // remove resource
  bool resourceRemove(1: string sessionId, 2: string paragraphId, 3:string resourceName) throws (1: InterpreterRPCException ex);
  // invoke method on resource
  binary resourceInvokeMethod(1: string sessionId, 2: string paragraphId, 3:string resourceName, 4:string invokeMessage) throws (1: InterpreterRPCException ex);

  void angularObjectUpdate(1: string name, 2: string sessionId, 3: string paragraphId, 4: string object) throws (1: InterpreterRPCException ex);
  void angularObjectAdd(1: string name, 2: string sessionId, 3: string paragraphId, 4: string object) throws (1: InterpreterRPCException ex);
  void angularObjectRemove(1: string name, 2: string sessionId, 3: string paragraphId) throws (1: InterpreterRPCException ex);
  void angularRegistryPush(1: string registry) throws (1: InterpreterRPCException ex);
}


