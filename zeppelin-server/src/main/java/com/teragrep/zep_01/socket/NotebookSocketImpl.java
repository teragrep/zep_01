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
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WebSocketListener;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Notebook websocket.
 * Responsibilities:
 * - Receives messages from websocket client via {@link WebSocketListener}
 * - Sends messages to websocket client
 * - Reports whether the connection is allowed to be turned into a Watcher
 * - Sets a username for the connection
 *
 * Collaborators:
 * - NotebookSocketListener - gets notified when a websocket message is received, a connection is opened or closed
 **/
public class NotebookSocketImpl implements NotebookSocket {
  private Session connection;
  private final NotebookSocketListener listener;
  private String user;
  private final HttpServletRequest request;

  public NotebookSocketImpl(final HttpServletRequest request,
                        NotebookSocketListener listener) {
    this.listener = listener;
    this.request = request;
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

  @Override
  public void onWebSocketError(final Throwable cause) {
    /* do nothing */
  }

  @Override
  public void onWebSocketBinary(final byte[] payload, final int offset, final int len) {
    /* do nothing */
  }

  @Override
  public synchronized void send(String serializeMessage) throws IOException {
    connection.getRemote().sendStringByFuture(serializeMessage);
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public void setUser(String user) {
    this.user = user;
  }

  @Override
  public HttpServletRequest getRequest() {
    return request;
  }

  @Override
  public String toString() {
    return request.getRemoteHost() + ":" + request.getRemotePort();
  }
}
