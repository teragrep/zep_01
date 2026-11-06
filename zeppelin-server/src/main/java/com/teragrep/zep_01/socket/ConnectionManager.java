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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.display.GUI;
import com.teragrep.zep_01.display.Input;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.NoteInfo;
import com.teragrep.zep_01.notebook.NotebookImportDeserializer;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.notebook.AuthorizationService;
import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.notebook.socket.WatcherMessage;
import com.teragrep.zep_01.user.AuthenticationInfo;
import com.teragrep.zep_01.util.WatcherSecurityKey;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manager class for managing websocket connections
 */
public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);
  private static final Gson gson = new GsonBuilder()
      .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
      .registerTypeAdapter(Date.class, new NotebookImportDeserializer())
      .setPrettyPrinting()
      .registerTypeAdapterFactory(Input.TypeAdapterFactory).create();

  final Queue<WebSocketEndpoint> connectedSockets = new ConcurrentLinkedQueue<>();
  // noteId -> connection
  final Map<String, List<WebSocketEndpoint>> noteSocketMap = new HashMap<>();
  // user -> connection
  final Map<String, Queue<WebSocketEndpoint>> userSocketMap = new HashMap<>();

  /**
   * This is a special endpoint in the notebook websoket, Every connection in this Queue
   * will be able to watch every websocket event, it doesnt need to be listed into the map of
   * noteSocketMap. This can be used to get information about websocket traffic and watch what
   * is going on.
   */
  final Queue<WebSocketEndpoint> watcherSockets = new ConcurrentLinkedQueue<>();

  private final Boolean collaborativeModeEnable = ZeppelinConfiguration
      .create()
      .isZeppelinNotebookCollaborativeModeEnable();


  private final AuthorizationService authorizationService;

  @Inject
  public ConnectionManager(final AuthorizationService authorizationService) {
    this.authorizationService = authorizationService;
  }

  public void addConnection(final WebSocketEndpoint conn) {
    connectedSockets.add(conn);
  }

  public void removeConnection(final WebSocketEndpoint conn) {
    connectedSockets.remove(conn);
  }

  public void addNoteConnection(final String noteId, final WebSocketEndpoint socket) {
    LOGGER.debug("Add connection <{}> : <{}> to note: <{}>", socket.getRequest().getRemoteHost(),socket.getRequest().getRemotePort(), noteId);
    synchronized (noteSocketMap) {
      // make sure a socket relates only an single note.
      removeConnectionFromAllNote(socket);
      final List<WebSocketEndpoint> socketList = noteSocketMap.computeIfAbsent(noteId, k -> new LinkedList<>());
      if (!socketList.contains(socket)) {
        socketList.add(socket);
      }
      checkCollaborativeStatus(noteId, socketList);
    }
  }

  public void removeNoteConnection(final String noteId) {
    synchronized (noteSocketMap) {
      noteSocketMap.remove(noteId);
    }
  }

  public void removeNoteConnection(final String noteId, final WebSocketEndpoint socket) {
    LOGGER.debug("Remove connection <{}> : <{}> from note: <{}>", socket.getRequest().getRemoteHost(),socket.getRequest().getRemotePort(), noteId);
    synchronized (noteSocketMap) {
      final List<WebSocketEndpoint> socketList = noteSocketMap.get(noteId);
      if (socketList != null && socketList.contains(socket)) {
        socketList.remove(socket);
        checkCollaborativeStatus(noteId, socketList);
      }
    }
  }

  public void addUserConnection(final WebSocketEndpoint conn) {
    final String user = conn.username().asString();
    LOGGER.debug("Add user connection <{}> : <{}> for user: <{}>", conn.getRequest().getRemoteHost(),conn.getRequest().getRemotePort(), user);
    if (userSocketMap.containsKey(user)) {
      userSocketMap.get(user).add(conn);
    } else {
      final Queue<WebSocketEndpoint> socketQueue = new ConcurrentLinkedQueue<>();
      socketQueue.add(conn);
      userSocketMap.put(user, socketQueue);
    }
  }

  public void removeUserConnection(final String user, final WebSocketEndpoint conn) {
    LOGGER.debug("Remove user connection <{}> : <{}> for user: <{}>", conn.getRequest().getRemoteHost(),conn.getRequest().getRemotePort(), user);
    if (userSocketMap.containsKey(user)) {
      userSocketMap.get(user).remove(conn);
    } else {
      LOGGER.warn("Closing connection that is absent in user connections");
    }
  }

  public String getAssociatedNoteId(final WebSocketEndpoint socket) {
    String associatedNoteId = null;
    synchronized (noteSocketMap) {
      final Set<String> noteIds = noteSocketMap.keySet();
      for (final String noteId : noteIds) {
        final List<WebSocketEndpoint> sockets = noteSocketMap.get(noteId);
        if (sockets.contains(socket)) {
          associatedNoteId = noteId;
        }
      }
    }

    return associatedNoteId;
  }

  public void removeConnectionFromAllNote(final WebSocketEndpoint socket) {
    synchronized (noteSocketMap) {
      final Set<String> noteIds = noteSocketMap.keySet();
      for (final String noteId : noteIds) {
        removeNoteConnection(noteId, socket);
      }
    }
  }

  // Sends a response message indicating whether or not the notebook in question has multiple users connected, and lists the usernames of the connected users
  private void checkCollaborativeStatus(final String noteId, final List<WebSocketEndpoint> socketList) {
    if (!collaborativeModeEnable) {
      return;
    }
    final boolean isCollaborative = socketList.size() > 1;
    final Message message = new Message(Message.OP.COLLABORATIVE_MODE_STATUS);
    message.put("status", isCollaborative);
    // Create a list of users for the response
    final Set<String> userList = new HashSet<>();
    for (final WebSocketEndpoint noteSocket : socketList) {
      userList.add(noteSocket.username().asString());
    }
    message.put("users", userList);
    broadcast(noteId, message);
  }


  protected String serializeMessage(final Message m) {
    return gson.toJson(m);
  }

  public void broadcast(final Message m) {
    synchronized (connectedSockets) {
      for (final WebSocketEndpoint ns : connectedSockets) {
        try {
          ns.send(serializeMessage(m));
        } catch (final IOException | WebSocketException e) {
          LOGGER.error("Send error: " + m, e);
        }
      }
    }
  }

  public void broadcast(final String noteId, final Message m) {
    List<WebSocketEndpoint> socketsToBroadcast = Collections.emptyList();
    synchronized (noteSocketMap) {
      broadcastToWatchers(noteId, StringUtils.EMPTY, m);
      final List<WebSocketEndpoint> socketLists = noteSocketMap.get(noteId);
      if (socketLists == null || socketLists.size() == 0) {
        return;
      }
      socketsToBroadcast = new ArrayList<>(socketLists);
    }
    LOGGER.debug("SEND >> " + m);
    for (final WebSocketEndpoint conn : socketsToBroadcast) {
      try {
        conn.send(serializeMessage(m));
      } catch (final IOException | WebSocketException e) {
        LOGGER.error("socket error", e);
      }
    }
  }

  public void broadcast(final String noteId, final String message){
    List<WebSocketEndpoint> socketsToBroadcast = Collections.emptyList();
    synchronized (noteSocketMap) {
      broadcastToWatchers(noteId, StringUtils.EMPTY, message);
      final List<WebSocketEndpoint> socketLists = noteSocketMap.get(noteId);
      if (socketLists == null || socketLists.size() == 0) {
        return;
      }
      socketsToBroadcast = new ArrayList<>(socketLists);
    }
    for (final WebSocketEndpoint conn : socketsToBroadcast) {
      try {
        conn.send(message);
      } catch (final IOException | WebSocketException e) {
        LOGGER.error("socket error", e);
      }
    }
  }

  private void broadcastToWatchers(final String noteId, final String subject, final Message message) {
    synchronized (watcherSockets) {
      for (final WebSocketEndpoint watcher : watcherSockets) {
        try {
          watcher.send(
              WatcherMessage.builder(noteId)
                  .subject(subject)
                  .message(serializeMessage(message))
                  .build()
                  .toJson());
        } catch (final IOException | WebSocketException e) {
          LOGGER.error("Cannot broadcast message to watcher", e);
        }
      }
    }
  }

  private void broadcastToWatchers(final String noteId, final String subject, final String message) {
    synchronized (watcherSockets) {
      for (final WebSocketEndpoint watcher : watcherSockets) {
        try {
          watcher.send(
                  WatcherMessage.builder(noteId)
                          .subject(subject)
                          .message(message)
                          .build()
                          .toJson());
        } catch (final IOException | WebSocketException e) {
          LOGGER.error("Cannot broadcast message to watcher", e);
        }
      }
    }
  }

  public void broadcastExcept(final String noteId, final Message m, final WebSocketEndpoint exclude) {
    List<WebSocketEndpoint> socketsToBroadcast = Collections.emptyList();
    synchronized (noteSocketMap) {
      broadcastToWatchers(noteId, StringUtils.EMPTY, m);
      final List<WebSocketEndpoint> socketLists = noteSocketMap.get(noteId);
      if (socketLists == null || socketLists.size() == 0) {
        return;
      }
      socketsToBroadcast = new ArrayList<>(socketLists);
    }

    LOGGER.debug("SEND >> " + m);
    for (final WebSocketEndpoint conn : socketsToBroadcast) {
      if (exclude.equals(conn)) {
        continue;
      }
      try {
        conn.send(serializeMessage(m));
      } catch (final IOException | WebSocketException e) {
        LOGGER.error("socket error", e);
      }
    }
  }

  /**
   * Send websocket message to all connections regardless of notebook id.
   */
  public void broadcastToAllConnections(final String serialized) {
    broadcastToAllConnectionsExcept(null, serialized);
  }

  public void broadcastToAllConnectionsExcept(final WebSocketEndpoint exclude, final String serializedMsg) {
    synchronized (connectedSockets) {
      for (final WebSocketEndpoint conn : connectedSockets) {
        if (exclude != null && exclude.equals(conn)) {
          continue;
        }

        try {
          conn.send(serializedMsg);
        } catch (final IOException | WebSocketException e) {
          LOGGER.error("Cannot broadcast message to conn", e);
        }
      }
    }
  }

  public Set<String> getConnectedUsers() {
    final Set<String> connectedUsers = new HashSet<>();
    for (final WebSocketEndpoint notebookSocket : connectedSockets) {
      connectedUsers.add(notebookSocket.username().asString());
    }
    return connectedUsers;
  }


  public void multicastToUser(final String user, final Message m) {
    if (!userSocketMap.containsKey(user)) {
      LOGGER.warn("Multicasting to user {} that is not in connections map", user);
      return;
    }

    for (final WebSocketEndpoint conn : userSocketMap.get(user)) {
      unicast(m, conn);
    }
  }

  public void multicastToUser(final String user, final String m) {
    if (!userSocketMap.containsKey(user)) {
      LOGGER.warn("Multicasting to user {} that is not in connections map", user);
      return;
    }

    for (final WebSocketEndpoint conn : userSocketMap.get(user)) {
      unicast(m, conn);
    }
  }

  public void unicast(final Message m, final WebSocketEndpoint conn) {
    try {
      conn.send(serializeMessage(m));
    } catch (final IOException | WebSocketException e) {
      LOGGER.error("socket error", e);
    }
    broadcastToWatchers(StringUtils.EMPTY, StringUtils.EMPTY, m);
  }
  public void unicast(final String m, final WebSocketEndpoint conn) {
    try {
      conn.send(m);
    } catch (final IOException | WebSocketException e) {
      LOGGER.error("socket error", e);
    }
    broadcastToWatchers(StringUtils.EMPTY, StringUtils.EMPTY, m);
  }

  public void unicastParagraph(final Note note, final Paragraph p, final String user, final String msgId) {
    if (!note.isPersonalizedMode() || p == null || user == null) {
      return;
    }

    if (!userSocketMap.containsKey(user)) {
      LOGGER.warn("Failed to send unicast. user {} that is not in connections map", user);
      return;
    }

    for (final WebSocketEndpoint conn : userSocketMap.get(user)) {
      final Message m = new Message(Message.OP.PARAGRAPH).withMsgId(msgId).put("paragraph", p);
      unicast(m, conn);
    }
  }

  public interface UserIterator {
    void handleUser(String user, Set<String> userAndRoles);
  }

  public void forAllUsers(final UserIterator iterator) {
    for (final String user : userSocketMap.keySet()) {
      final Set<String> userAndRoles = authorizationService.getRoles(user);
      userAndRoles.add(user);
      iterator.handleUser(user, userAndRoles);
    }
  }

  public void broadcastNoteListExcept(final List<NoteInfo> notesInfo,
                                      final AuthenticationInfo subject) {
    Set<String> userAndRoles;
    for (final String user : userSocketMap.keySet()) {
      if (subject.getUser().equals(user)) {
        continue;
      }
      //reloaded already above; parameter - false
      userAndRoles = authorizationService.getRoles(user);
      userAndRoles.add(user);
      // TODO(zjffdu) is it ok for comment the following line ?
      // notesInfo = generateNotesInfo(false, new AuthenticationInfo(user), userAndRoles);
      multicastToUser(user, new Message(Message.OP.NOTES_INFO).put("notes", notesInfo));
    }
  }

  public void broadcastNote(final Note note) {
    broadcast(note.getId(), new Message(Message.OP.NOTE).put("note", note));
  }

  public void broadcastParagraph(final Note note, final Paragraph p) {
    broadcastNoteForms(note);

    if (note.isPersonalizedMode()) {
      broadcastParagraphs(p.getUserParagraphMap());
    } else {
      broadcast(note.getId(), new Message(Message.OP.PARAGRAPH).put("paragraph", p));
    }
  }

  public void broadcastParagraphs(final Map<String, Paragraph> userParagraphMap) {
    if (null != userParagraphMap) {
      for (final String user : userParagraphMap.keySet()) {
        multicastToUser(user,
            new Message(Message.OP.PARAGRAPH).put("paragraph", userParagraphMap.get(user)));
      }
    }
  }

  private void broadcastNewParagraph(final Note note, final Paragraph para) {
    LOGGER.info("Broadcasting paragraph on run call instead of note.");
    final int paraIndex = note.getParagraphs().indexOf(para);
    broadcast(note.getId(),
        new Message(Message.OP.PARAGRAPH_ADDED).put("paragraph", para).put("index", paraIndex));
  }

  //  public void broadcastNoteList(AuthenticationInfo subject, Set<String> userAndRoles) {
  //    if (subject == null) {
  //      subject = new AuthenticationInfo(StringUtils.EMPTY);
  //    }
  //    //send first to requesting user
  //    List<Map<String, String>> notesInfo = generateNotesInfo(false, subject, userAndRoles);
  //    multicastToUser(subject.getUser(), new Message(Message.OP.NOTES_INFO)
  // .put("notes", notesInfo));
  //    //to others afterwards
  //    broadcastNoteListExcept(notesInfo, subject);
  //  }


  private void broadcastNoteForms(final Note note) {
    final GUI formsSettings = new GUI();
    formsSettings.setForms(note.getNoteForms());
    formsSettings.setParams(note.getNoteParams());
    broadcast(note.getId(), new Message(Message.OP.SAVE_NOTE_FORMS)
        .put("formsData", formsSettings));
  }

  public void switchConnectionToWatcher(final WebSocketEndpoint conn) {
    if (!isSessionAllowedToSwitchToWatcher(conn)) {
      LOGGER.error("Cannot switch this client to watcher, invalid security key");
      return;
    }
    LOGGER.info("Going to add {} to watcher socket", conn);
    // add the connection to the watcher.
    if (watcherSockets.contains(conn)) {
      LOGGER.info("connection already present in the watcher");
      return;
    }
    watcherSockets.add(conn);

    // remove this connection from regular zeppelin ws usage.
    removeConnection(conn);
    removeConnectionFromAllNote(conn);
    removeUserConnection(conn.username().asString(), conn);
  }

  private boolean isSessionAllowedToSwitchToWatcher(final WebSocketEndpoint session) {
    final String watcherSecurityKey = session.getRequest().getHeader(WatcherSecurityKey.HTTP_HEADER);
    return !(StringUtils.isBlank(watcherSecurityKey) || !watcherSecurityKey
        .equals(WatcherSecurityKey.getKey()));
  }
}
