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

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;

import java.util.Objects;


/**
 * A Message that contains a single Jsonable as its data.
 */
public final class JsonMessage implements Jsonable {

  private final MessageId id;
  private final Message.OP op;
  private final JsonValue data;
  private final String ticket;
  private final String principal;
  private final String roles;

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final JsonMessage that = (JsonMessage) o;
    return Objects.equals(id, that.id) && op == that.op && Objects.equals(data, that.data) && Objects.equals(ticket, that.ticket) && Objects.equals(principal, that.principal) && Objects.equals(roles, that.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, op, data, ticket, principal, roles);
  }

  public JsonMessage(final Message.OP op, final JsonValue data){
    this(new StubMessageId(), op,data,"anonymous","anonymous","");
  }
  public JsonMessage(final MessageId id, final Message.OP op, final JsonValue data){
    this(id, op,data,"anonymous","anonymous","");
  }
  public JsonMessage(final Message.OP op, final Jsonable data){
    this(new StubMessageId(), op,data.asJson(),"anonymous","anonymous","");
  }
  public JsonMessage(final MessageId id, final Message.OP op, final Jsonable data){
    this(id, op,data.asJson(),"anonymous","anonymous","");
  }

  public JsonMessage(final MessageId id, final Message.OP op, final JsonValue data, final String ticket, final String principal, final String roles) {
    this.id = id;
    this.op = op;
    this.data = data;
    this.ticket = ticket;
    this.principal = principal;
    this.roles = roles;
  }
  @Override
  public JsonObject asJson() {
    final JsonObjectBuilder json = Json.createObjectBuilder();
    if(!id.isStub()){
      json.add("msgId",id.asJson());
    }
    json.add("op",op.toString());
    json.add("data",data);
    json.add("ticket",ticket);
    json.add("principal",principal);
    json.add("roles",roles);
    return json.build();
  }
}
