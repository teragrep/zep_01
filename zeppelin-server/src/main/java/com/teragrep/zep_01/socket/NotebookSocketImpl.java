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

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Notebook websocket.
 * Responsibilities:
 * - Receives messages from websocket client via {@link WebSocketListener}
 * - Assigns a {@link Session} via {@link org.eclipse.jetty.websocket.api.WebSocketConnectionListener} to facilitate communication with client
 * - Sends messages to websocket client
 *
 * Collaborators:
 * - {@link NotebookSocketListener} - Gets notified when a websocket message is received, a connection is opened or closed
 * - {@link HttpServletRequest} - HTTP request that was upgraded to a Websocket connection
 **/
public final class NotebookSocketImpl implements NotebookSocket {
  private final AtomicReference<Session> connection;  // use of AtomicReference here due to Jetty's WebSocketConnectionListener assigns a Session object via onWebSocketConnect()
  private final NotebookSocketListener listener;
  private final AtomicReference<Username> username;
  private final HttpServletRequest request;

  public NotebookSocketImpl(final HttpServletRequest request,
                            final NotebookSocketListener listener) {
    this(request,listener,new UsernameAnonymous());
  }

  public NotebookSocketImpl(final HttpServletRequest request,
                            final NotebookSocketListener listener,
                            final Username username) {
    this.connection = new AtomicReference<>();
    this.listener = listener;
    this.request = request;
    this.username = new AtomicReference<>(username);
  }

  @Override
  public void onWebSocketClose(final int closeCode, final String message) {
    listener.onClose(this, closeCode, message);
  }

  @Override
  public void onWebSocketConnect(final Session connection) {
    this.connection.set(connection);
    listener.onOpen(this);
  }

  @Override
  public void onWebSocketText(final String message) {
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
  public synchronized void send(final String serializeMessage) throws IOException {
    final Session connection = this.connection.get();
    if(connection == null || !connection.isOpen()){
      throw new IOException("Tried to send a message to a Websocket connection with no established Session!");
    }
    connection.getRemote().sendStringByFuture(serializeMessage);
  }

  @Override
  public Username username() {
    return username.get();
  }

  @Override
  public void rename(Username username) {
    this.username.set(username);
  }

  @Override
  public HttpServletRequest getRequest() {
    return request;
  }

  @Override
  public boolean equals(final Object o) {
    final boolean equals;
    if (this == o) {
      equals = true;
    } else if (o == null || getClass() != o.getClass()) {
      equals = false;
    } else {
      final NotebookSocketImpl that = (NotebookSocketImpl) o;
      equals = Objects.equals(connection != null ? this.connection.get() : null, that.connection != null ? that.connection.get() : null) && Objects.equals(listener, that.listener) && Objects.equals(username != null ? this.username.get() : null, that.username != null ? that.username.get() : null) && Objects.equals(request, that.request);
    }
    return equals;
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection != null ? connection.get() : null, listener, username != null ? username.get() : null, request);
  }
}
