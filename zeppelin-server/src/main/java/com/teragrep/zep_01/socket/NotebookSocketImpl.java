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
package com.teragrep.zep_01.socket;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

import java.io.IOException;

/**
 * Notebook websocket.
 */
public class NotebookSocketImpl extends WebSocketAdapter implements NotebookSocket {
  private Session connection;
  private final NotebookSocketListener listener;
  private final String host;
  private final int port;
  private String user;
  private final boolean watcherPermitted;

  public NotebookSocketImpl(String host, int port,
                        NotebookSocketListener listener, boolean watcherPermitted) {
    this.listener = listener;
    this.host = host;
    this.port = port;
    this.watcherPermitted = watcherPermitted;
    this.user = StringUtils.EMPTY;
  }

  @Override
  public void onWebSocketClose(int closeCode, String message) {
    listener.onClose(this, closeCode, message);
  }

  @Override
  public void onWebSocketConnect(Session connection) {
    this.connection = connection;
    listener.onOpen(this);
  }

  @Override
  public void onWebSocketText(String message) {
    listener.onMessage(this, message);
  }

  public synchronized void send(String serializeMessage) throws IOException {
    connection.getRemote().sendStringByFuture(serializeMessage);
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public boolean watcherPermitted(){
    return watcherPermitted;
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }
}
