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
package com.teragrep.zep_01.rest;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonSyntaxException;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import com.teragrep.zep_01.service.AuthenticationService;
import com.teragrep.zep_01.service.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.teragrep.zep_01.annotation.ZeppelinApi;
import com.teragrep.zep_01.notebook.repo.NotebookRepoSync;
import com.teragrep.zep_01.notebook.repo.NotebookRepoWithSettings;
import com.teragrep.zep_01.rest.message.NotebookRepoSettingsRequest;
import com.teragrep.zep_01.server.JsonResponse;
import com.teragrep.zep_01.socket.NotebookServer;
import com.teragrep.zep_01.user.AuthenticationInfo;

/**
 * NoteRepo rest API endpoint.
 *
 */
@Path("/notebook-repositories")
@Produces("application/json")
@Singleton
public class NotebookRepoRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRepoRestApi.class);

  private final NotebookRepoSync noteRepos;
  private final NotebookServer notebookWsServer;
  private final AuthenticationService authenticationService;

  @Inject
  public NotebookRepoRestApi(NotebookRepoSync noteRepos, NotebookServer notebookWsServer,
      AuthenticationService authenticationService) {
    this.noteRepos = noteRepos;
    this.notebookWsServer = notebookWsServer;
    this.authenticationService = authenticationService;
  }

  /**
   * List all notebook repository.
   */
  @GET
  @ZeppelinApi
  public Response listRepoSettings() {
    AuthenticationInfo subject = new AuthenticationInfo(authenticationService.getPrincipal());
    LOG.info("Getting list of NoteRepo with Settings for user {}", subject.getUser());
    List<NotebookRepoWithSettings> settings = noteRepos.getNotebookRepos(subject);
    return new JsonResponse<>(Status.OK, "", settings).build();
  }

  /**
   * Reload notebook repository.
   */
  @GET
  @Path("reload")
  @ZeppelinApi
  public Response refreshRepo(){
    AuthenticationInfo subject = new AuthenticationInfo(authenticationService.getPrincipal());
    LOG.info("Reloading notebook repository for user {}", subject.getUser());
    try {
      notebookWsServer.broadcastReloadedNoteList(getServiceContext());
    } catch (IOException e) {
      LOG.error("Fail to refresh repo", e);
    }
    return new JsonResponse<>(Status.OK, "", null).build();
  }

  private ServiceContext getServiceContext() {
    AuthenticationInfo authInfo = new AuthenticationInfo(authenticationService.getPrincipal());
    Set<String> userAndRoles = new HashSet<>();
    userAndRoles.add(authenticationService.getPrincipal());
    userAndRoles.addAll(authenticationService.getAssociatedRoles());
    return new ServiceContext(authInfo, userAndRoles);
  }

  /**
   * Update a specific note repo.
   *
   * @param payload
   * @return
   */
  @PUT
  @ZeppelinApi
  public Response updateRepoSetting(String payload) {
    if (StringUtils.isBlank(payload)) {
      return new JsonResponse<>(Status.NOT_FOUND, "", Collections.emptyMap()).build();
    }
    AuthenticationInfo subject = new AuthenticationInfo(authenticationService.getPrincipal());
    NotebookRepoSettingsRequest newSettings;
    try {
      newSettings = NotebookRepoSettingsRequest.fromJson(payload);
    } catch (JsonSyntaxException e) {
      LOG.error("Cannot update notebook repo settings", e);
      return new JsonResponse<>(Status.NOT_ACCEPTABLE, "",
          ImmutableMap.of("error", "Invalid payload structure")).build();
    }

    if (NotebookRepoSettingsRequest.isEmpty(newSettings)) {
      LOG.error("Invalid property");
      return new JsonResponse<>(Status.NOT_ACCEPTABLE, "",
          ImmutableMap.of("error", "Invalid payload")).build();
    }
    LOG.info("User {} is going to change repo setting", subject.getUser());
    NotebookRepoWithSettings updatedSettings =
        noteRepos.updateNotebookRepo(newSettings.name, newSettings.settings, subject);
    if (!updatedSettings.isEmpty()) {
      LOG.info("Broadcasting note list to user {}", subject.getUser());
      try {
        notebookWsServer.broadcastReloadedNoteList(getServiceContext());
      } catch (IOException e) {
        LOG.error("Fail to refresh repo.", e);
      }
    }
    return new JsonResponse<>(Status.OK, "", updatedSettings).build();
  }
}
