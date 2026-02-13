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

package com.teragrep.zep_01.common;

import com.google.gson.Gson;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.slf4j.Logger;

import java.util.*;

/**
 * A Message that contains a single Jsonable as it's data.
 */
public class JsonMessage implements Jsonable {

  private Message.OP op;
  private Jsonable data;
  private String ticket = "anonymous";
  private String principal = "anonymous";
  private String roles = "";

  // Unique id generated from client side. to identify message.
  // When message from server is response to the client request
  // includes the msgId in response message, client can pair request and response message.
  // When server send message that is not response to the client request, set null;
  public String msgId = MSG_ID_NOT_DEFINED;
  public static String MSG_ID_NOT_DEFINED = null;

  public JsonMessage(Message.OP op, Jsonable data){
    this(op,data,"anonymous","anonymous","");
  }

  public JsonMessage(Message.OP op, Jsonable data, String ticket, String principal, String roles) {
    this.op = op;
    this.data = data;
    this.ticket = ticket;
    this.principal = principal;
    this.roles = roles;
  }
  @Override
  public JsonObject asJson() {
    JsonObject json = Json.createObjectBuilder()
            .add("op",op.toString())
            .add("data",data.asJson())
            .add("ticket",ticket)
            .add("principal",principal)
            .add("roles",roles).build();
    return json;
  }
}
