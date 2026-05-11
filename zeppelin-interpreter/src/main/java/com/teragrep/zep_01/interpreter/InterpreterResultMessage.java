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
package com.teragrep.zep_01.interpreter;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.*;

import java.io.Serializable;
import java.io.StringReader;

/**
 * Interpreter result message
 */
public class InterpreterResultMessage implements Serializable, Jsonable {
  InterpreterResult.Type type;
  String data;

  public InterpreterResultMessage(InterpreterResult.Type type, String data) {
    this.type = type;
    this.data = data;
  }

  public InterpreterResult.Type getType() {
    return type;
  }

  public String getData() {
    return data;
  }

  public String toString() {
    return "%" + type.name().toLowerCase() + " " + data;
  }

  @Override
  public JsonObject asJson() {
    // If the data within this resultMessage is in a JSON formatted type, read the String into a JSON object.
    // If the data is in some other format, create a JSON object with the keys expected by UI, and the data as a simple String.
    final JsonObject rv;
    if(type.equals(InterpreterResult.Type.DATATABLES) || type.equals(InterpreterResult.Type.UPLOT)){
      rv = Json.createReader(new StringReader(data)).readObject();
    }
    else {
      JsonObjectBuilder builder = Json.createObjectBuilder();
      builder.add("data",data);
      builder.add("type",type.label);
      builder.add("isAggregated",false);
      rv = builder.build();
    }
    return rv;
  }
}
