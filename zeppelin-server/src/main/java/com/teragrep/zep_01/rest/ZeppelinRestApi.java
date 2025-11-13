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

import javax.inject.Singleton;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.teragrep.zep_01.annotation.ZeppelinApi;
import com.teragrep.zep_01.server.JsonResponse;
import com.teragrep.zep_01.util.Util;
import org.slf4j.LoggerFactory;

/**
 * Zeppelin root rest api endpoint.
 *
 * @since 0.3.4
 */
@Path("/")
@Singleton
public class ZeppelinRestApi {

  org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());

  /**
   * Get the root endpoint Return always 200.
   *
   * @return 200 response
   */
  @GET
  public Response getRoot() {
    return Response.ok().build();
  }

  @GET
  @Path("version")
  @ZeppelinApi
  public Response getVersion() {
    Map<String, String> versionInfo = new HashMap<>();
    versionInfo.put("version", Util.getVersion());
    versionInfo.put("git-commit-id", Util.getGitCommitId());
    versionInfo.put("git-timestamp", Util.getGitTimestamp());
    return new JsonResponse<>(Response.Status.OK, "Zeppelin version", versionInfo).build();
  }

  /**
   * Gets the most recent announcement text.
   *
   * @return Most recent Announcement text, prioritizing values from Environment variables (set in zeppelin-env.sh), secondarily from System properties (set in zeppelin-site.xml)
   */
  @GET
  @Path("announcement")
  @ZeppelinApi
  public Response getAnnouncement() {
    Map<String, String> json = new HashMap<>();
    // Searches first from Environment variables, if a match is not found, searches from zeppelin-site.xml, if a match is not found, returns a default value.
    String announcementText = ZeppelinConfiguration.create().getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ANNOUNCEMENT);
    json.put("announcement", announcementText);
    return new JsonResponse<>(Response.Status.OK, json).build();
  }

  /**
   * Set a new value for announcement text. Does not override announcement texts from Environment variables (set via zeppelin-env.sh)
   *
   * @param request
   * @return
   */
  @PUT
  @Path("announcement")
  public Response setAnnouncement(@Context HttpServletRequest request) {
    Response response;
    String envAnnouncement = System.getenv(ZeppelinConfiguration.ConfVars.ZEPPELIN_ANNOUNCEMENT.getVarName());
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()))){
      if(envAnnouncement == null){
        StringBuilder body = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null){
            body.append(line);
          }
          System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_ANNOUNCEMENT.getVarName(),body.toString());
          response = new JsonResponse<>(Response.Status.OK,"Announcement text set successfully").build();
        }
        else {
          response = new JsonResponse<>(Response.Status.BAD_REQUEST,"Announcement already set via environment variable!").build();
        }
    }
    catch (IOException e) {
      LOGGER.error("Error while setting announcement text!",e);
      response = new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    return response;
  }

  /**
   * Set the log level for root logger.
   *
   * @param request
   * @param logLevel new log level for Rootlogger
   * @return
   */
  @PUT
  @Path("log/level/{logLevel}")
  public Response changeRootLogLevel(@Context HttpServletRequest request,
      @PathParam("logLevel") String logLevel) {
    Level level = Level.toLevel(logLevel);
    if (logLevel.toLowerCase().equalsIgnoreCase(level.toString().toLowerCase())) {
      Logger.getRootLogger().setLevel(level);
      return new JsonResponse<>(Response.Status.OK).build();
    } else {
      return new JsonResponse<>(Response.Status.NOT_ACCEPTABLE,
          "Please check LOG level specified. Valid values: DEBUG, ERROR, FATAL, "
              + "INFO, TRACE, WARN").build();
    }
  }
}
