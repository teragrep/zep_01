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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.teragrep.zep_01.interpreter.*;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.rest.message.*;
import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonParsingException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import com.teragrep.zep_01.annotation.ZeppelinApi;
import com.teragrep.zep_01.notebook.AuthorizationService;
import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.common.Message.OP;
import com.teragrep.zep_01.server.JsonResponse;
import com.teragrep.zep_01.service.AuthenticationService;
import com.teragrep.zep_01.service.InterpreterService;
import com.teragrep.zep_01.service.ServiceContext;
import com.teragrep.zep_01.service.SimpleServiceCallback;
import com.teragrep.zep_01.socket.NotebookServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.aether.repository.RemoteRepository;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interpreter Rest API.
 */
@Path("/interpreter")
@Produces("application/json")
@Singleton
public class InterpreterRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterRestApi.class);

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final InterpreterService interpreterService;
  private final InterpreterSettingManager interpreterSettingManager;
  private final NotebookServer notebookServer;

  @Inject
  public InterpreterRestApi(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      InterpreterService interpreterService,
      InterpreterSettingManager interpreterSettingManager,
      NotebookServer notebookWsServer) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.interpreterService = interpreterService;
    this.interpreterSettingManager = interpreterSettingManager;
    this.notebookServer = notebookWsServer;
  }

  /**
   * List all interpreter settings.
   */
  @GET
  @Path("setting")
  @ZeppelinApi
  public Response listSettings() {
    return new JsonResponse<>(Status.OK, "", interpreterSettingManager.get()).build();
  }

  /**
   * Get a setting.
   */
  @GET
  @Path("setting/{settingId}")
  @ZeppelinApi
  public Response getSetting(@PathParam("settingId") String settingId) {
    try {
      InterpreterSetting setting = interpreterSettingManager.get(settingId);
      if (setting == null) {
        return new JsonResponse<>(Status.NOT_FOUND).build();
      } else {
        return new JsonResponse<>(Status.OK, "", setting).build();
      }
    } catch (NullPointerException e) {
      LOGGER.error("Exception in InterpreterRestApi while creating ", e);
      return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new interpreter setting.
   *
   * @param message NewInterpreterSettingRequest
   */
  @POST
  @Path("setting")
  @ZeppelinApi
  public Response newSettings(String message) {
    try {
      NewInterpreterSettingRequest request =
          NewInterpreterSettingRequest.fromJson(message);
      if (request == null) {
        return new JsonResponse<>(Status.BAD_REQUEST).build();
      }

      InterpreterSetting interpreterSetting = interpreterSettingManager
          .createNewSetting(request.getName(), request.getGroup(),
              request.getOption(), request.getProperties());
      LOGGER.info("new setting created with {}", interpreterSetting.getId());
      return new JsonResponse<>(Status.OK, "", interpreterSetting).build();
    } catch (IOException e) {
      LOGGER.error("Exception in InterpreterRestApi while creating ", e);
      return new JsonResponse<>(Status.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
          .build();
    }
  }

  @PUT
  @Path("setting/{settingId}")
  @ZeppelinApi
  public Response updateSetting(String message, @PathParam("settingId") String settingId) {
    LOGGER.info("Update interpreterSetting {}", settingId);

    try {
      UpdateInterpreterSettingRequest request =
          UpdateInterpreterSettingRequest.fromJson(message);
      interpreterSettingManager
          .setPropertyAndRestart(settingId, request.getOption(), request.getProperties());
    } catch (InterpreterException e) {
      LOGGER.error("Exception in InterpreterRestApi while updateSetting ", e);
      return new JsonResponse<>(Status.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
          .build();
    } catch (IOException e) {
      LOGGER.error("Exception in InterpreterRestApi while updateSetting ", e);
      return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
    InterpreterSetting setting = interpreterSettingManager.get(settingId);
    if (setting == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "", settingId).build();
    }
    return new JsonResponse<>(Status.OK, "", setting).build();
  }

  /**
   * Remove interpreter setting.
   */
  @DELETE
  @Path("setting/{settingId}")
  @ZeppelinApi
  public Response removeSetting(@PathParam("settingId") String settingId) throws IOException {
    LOGGER.info("Remove interpreterSetting {}", settingId);
    interpreterSettingManager.remove(settingId);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Open interpreter setting.
   */
  @PUT
  @Path("setting/open/{settingId}")
  @ZeppelinApi
  public Response openSetting(String message, @PathParam("settingId") String settingId) {
    final Response response;
    try {
      // parse message
      final String userName = authenticationService.getPrincipal();
      final JsonObject json = Json.createReader(new StringReader(message)).readObject();
      final OpenInterpreterRequest request = new OpenInterpreterRequest(json);
      final String noteId = request.getNoteId();
      final Note note = notebookServer.getNotebook().getNote(noteId);
      final InterpreterSetting setting = interpreterSettingManager.get(settingId);

      // check permissions and nulls
      final Set<String> entities = new HashSet<>();
      entities.add(userName);
      entities.addAll(authenticationService.getAssociatedRoles());
      if (!authorizationService.hasRunPermission(entities, noteId) && !authorizationService.hasWritePermission(entities, noteId) && !authorizationService.isOwner(entities, noteId)) {
        OpenInterpreterResponse interpreterResponse = new OpenInterpreterResponse(Status.UNAUTHORIZED,"ERROR","No permission to open Interpreter " + settingId);
        throw new NotAuthorizedException(interpreterResponse.toResponse());
      }
      if (setting == null) {
        OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.NOT_FOUND,"ERROR","No such InterpreterSetting " + settingId);
        throw new NotFoundException(errorJson.toResponse());
      }
      if (noteId == null || note == null) {
        OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.NOT_FOUND,"ERROR","No such note " + noteId);
        throw new NotFoundException(errorJson.toResponse());
      }

      // check for presence of ConfInterpreter
      for (Paragraph paragraph:note.getParagraphs()) {
        if(paragraph.getBindedInterpreter() instanceof ConfInterpreter){
          OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.BAD_REQUEST,"ERROR","Cannot open Interpreter! Note "+noteId+" contains a paragraph with a ConfInterpreter!");
          throw new BadRequestException(errorJson.toResponse());
        }
      }

      // get interpreter instance and open
      Interpreter defaultInterpreter = setting.getDefaultInterpreter(authenticationService.getPrincipal(), noteId);
      defaultInterpreter.open();
      response = new OpenInterpreterResponse(Status.OK,setting.getStatus().toString(),"").toResponse();
    }
    catch (JsonParsingException jsonParsingException) {
      OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.BAD_REQUEST,"ERROR","Malformed request");
      throw new BadRequestException(errorJson.toResponse());
    }
    catch (IOException ioException) {
      LOGGER.error("Failed to get notebook while opening Interpreter <[{}]>",settingId,ioException);
      OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.INTERNAL_SERVER_ERROR,"ERROR","Internal server error while opening Interpreter "+settingId+"! Check technical logs for details.");
      throw new InternalServerErrorException(errorJson.toResponse());
    }
    catch (InterpreterException interpreterException){
      LOGGER.error("Failed to open Interpreter <[{}]>",settingId,interpreterException);
      OpenInterpreterResponse errorJson = new OpenInterpreterResponse(Status.INTERNAL_SERVER_ERROR,"ERROR","Internal server error while opening Interpreter "+settingId+"! Check technical logs for details.");
      throw new InternalServerErrorException(errorJson.toResponse());
    }
    return response;
  }

  /**
   * Restart interpreter setting.
   */
  @PUT
  @Path("setting/restart/{settingId}")
  @ZeppelinApi
  public Response restartSetting(String message, @PathParam("settingId") String settingId) {
    LOGGER.info("Restart interpreterSetting {}, msg={}", settingId, message);

    InterpreterSetting setting = interpreterSettingManager.get(settingId);
    try {
      RestartInterpreterRequest request = RestartInterpreterRequest.fromJson(message);

      String noteId = request == null ? null : request.getNoteId();
      if (null == noteId) {
        interpreterSettingManager.close(settingId);
      } else {
        Set<String> entities = new HashSet<>();
        entities.add(authenticationService.getPrincipal());
        entities.addAll(authenticationService.getAssociatedRoles());
        if (authorizationService.hasRunPermission(entities, noteId) ||
                authorizationService.hasWritePermission(entities, noteId) ||
                authorizationService.isOwner(entities, noteId)) {
          interpreterSettingManager.restart(settingId, authenticationService.getPrincipal(), noteId);
        } else {
          return new JsonResponse<>(Status.FORBIDDEN, "No privilege to restart interpreter")
                  .build();
        }
      }
    } catch (InterpreterException e) {
      LOGGER.error("Exception in InterpreterRestApi while restartSetting ", e);
      return new JsonResponse<>(Status.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
          .build();
    }
    if (setting == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "", settingId).build();
    }
    return new JsonResponse<>(Status.OK, "", setting).build();
  }

  /**
   * List all available interpreters by group.
   */
  @GET
  @ZeppelinApi
  public Response listInterpreter() {
    Map<String, InterpreterSetting> m = interpreterSettingManager.getInterpreterSettingTemplates();
    return new JsonResponse<>(Status.OK, "", m).build();
  }

  /**
   * Get available types for property
   */
  @GET
  @Path("property/types")
  public Response listInterpreterPropertyTypes() {
    return new JsonResponse<>(Status.OK, InterpreterPropertyType.getTypes()).build();
  }

  /** Install interpreter */
  @POST
  @Path("install")
  @ZeppelinApi
  public Response installInterpreter(@NotNull String message) {
    LOGGER.info("Install interpreter: {}", message);
    InterpreterInstallationRequest request = InterpreterInstallationRequest.fromJson(message);

    try {
      interpreterService.installInterpreter(
          request,
          new SimpleServiceCallback<String>() {
            @Override
            public void onStart(String message, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_STARTED);
              Map<String, Object> data = new HashMap<>();
              data.put("result", "Starting");
              data.put("message", message);
              m.data = data;
              notebookServer.broadcast(m);
            }

            @Override
            public void onSuccess(String message, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_RESULT);
              Map<String, Object> data = new HashMap<>();
              data.put("result", "Succeed");
              data.put("message", message);
              m.data = data;
              notebookServer.broadcast(m);
            }

            @Override
            public void onFailure(Exception ex, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_RESULT);
              Map<String, Object> data = new HashMap<>();
              data.put("result", "Failed");
              data.put("message", ex.getMessage());
              m.data = data;
              notebookServer.broadcast(m);
            }
          });
    } catch (Throwable t) {
      return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR, t.getMessage()).build();
    }

    return new JsonResponse<>(Status.OK).build();
  }
}
