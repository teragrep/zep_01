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
    JsonObjectBuilder resultBuilder = Json.createObjectBuilder();
    resultBuilder.add("type",type.label);
    // If the result is a valid JSON object, parse its contents into proper format.
    try{
      JsonObject messageAsJson = Json.createReader(new StringReader(data)).readObject();
      if(messageAsJson.containsKey("isAggregated")){
        JsonValue.ValueType isAggregatedType = messageAsJson.get("isAggregated").getValueType();
        if(isAggregatedType.equals(JsonValue.ValueType.TRUE) || isAggregatedType.equals(JsonValue.ValueType.FALSE)){
          boolean isAggregated = messageAsJson.getBoolean("isAggregated");
          resultBuilder.add("isAggregated",isAggregated);
        }
      }
      if(messageAsJson.containsKey("data")){
        resultBuilder.add("data",messageAsJson.get("data"));
      }
    }
    // Results are not always valid json, for example in cases where a stack trace is printed. In that case the data is simply added as a String.
    catch (JsonException jsonException){
      resultBuilder.add("data",data);
    }
    return resultBuilder.build();
  }
}
