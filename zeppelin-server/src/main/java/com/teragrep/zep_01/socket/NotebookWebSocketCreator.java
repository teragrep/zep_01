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

import com.teragrep.zep_01.util.WatcherSecurityKey;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import static com.teragrep.zep_01.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_ALLOWED_ORIGINS;

/**
 * Responsible to create the WebSockets for the NotebookServer.
 */
public class NotebookWebSocketCreator implements WebSocketCreator {

  private static final Logger LOG = LoggerFactory.getLogger(NotebookWebSocketCreator.class);
  private final NotebookServer notebookServer;

  public NotebookWebSocketCreator(NotebookServer notebookServer) {
    this.notebookServer = notebookServer;
  }

  public Object createWebSocket(ServletUpgradeRequest request, ServletUpgradeResponse response) {
    String origin = request.getHeader("Origin");
    HttpServletRequest servletRequest = request.getHttpServletRequest();
    if (notebookServer.checkOrigin(servletRequest, origin)) {
      final String watcherKey = servletRequest.getHeader(WatcherSecurityKey.HTTP_HEADER);
      final boolean watcherPermitted;
      if(!StringUtils.isBlank(watcherKey)){
        watcherPermitted = watcherKey.equals(WatcherSecurityKey.getKey());
      }
      else {
        watcherPermitted = false;
      }
      return new NotebookSocketImpl(servletRequest.getRemoteHost(),servletRequest.getRemotePort(), notebookServer, watcherPermitted);
    } else {
      LOG.error("Websocket request is not allowed by {} settings. Origin: {}",
          ZEPPELIN_ALLOWED_ORIGINS, origin);
      return null;
    }
  }

}
