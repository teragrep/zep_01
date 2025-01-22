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

package org.apache.zeppelin.user;

import org.apache.zeppelin.interpreter.xref.user.UserCredentials;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User Credentials POJO
 */
public class UserCredentialsImpl implements UserCredentials {
  private Map<String, UsernamePassword> userCredentials = new ConcurrentHashMap<>();

  @Override
  public UsernamePassword getUsernamePassword(String entity) {
    return userCredentials.get(entity);
  }

  @Override
  public void putUsernamePassword(String entity, UsernamePassword up) {
    userCredentials.put(entity, up);
  }

  @Override
  public void removeUsernamePassword(String entity) {
    userCredentials.remove(entity);
  }

  @Override
  public boolean existUsernamePassword(String entity) {
    return userCredentials.containsKey(entity);
  }

  @Override
  public String toString() {
    return "UserCredentials{" +
        "userCredentials=" + userCredentials +
        '}';
  }
}
