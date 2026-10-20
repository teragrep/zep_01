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
package com.teragrep.zep_01.rest.message;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

/**
 * OpenInterpreter rest api response message.
 */
public class OpenInterpreterResponse {

  private final Response.Status httpCode;
  private final String message;
  private final String result;

  public OpenInterpreterResponse(Response.Status httpCode, String result, String message) {
    this.httpCode = httpCode;
    this.result = result;
    this.message = message;
  }

  public JsonObject toJson(){
    return Json.createObjectBuilder()
            .add("status", httpCode.toString())
            .add("result", result)
            .add("message",message)
            .build();
  }

  public Response toResponse(){
    Response.ResponseBuilder r = javax.ws.rs.core.Response.status(httpCode).entity(toJson().toString());
    return r.build();
  }
}
