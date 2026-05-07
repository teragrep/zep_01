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
import jakarta.json.stream.JsonParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.Option;
import java.io.Serializable;
import java.io.StringReader;
import java.util.IllegalFormatException;

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

  private JsonObject format(JsonObject resultAsJson, String options) throws InterpreterException {
    if(type == null){
      throw new InterpreterException("Result has no type assigned!");
    }
    JsonObject json = Json.createReader(new StringReader(options)).readObject();
    if (json.getString("type").equals("dataTables") && type.equals(InterpreterResult.Type.DATATABLES)){
      final JsonObject jsonOptions = json.getJsonObject("options");
      final int draw = jsonOptions.containsKey("draw") ? jsonOptions.getInt("draw") : 1;
      final int start = jsonOptions.containsKey("start") ? jsonOptions.getInt("start") : 0;
      final int length = jsonOptions.containsKey("length") ? jsonOptions.getInt("length") : 50;
      final JsonObject dataObject = resultAsJson.getJsonObject("data");
      final JsonArray dataArray = dataObject.getJsonArray("data");

      final int startIndex = Math.min(Math.max(start,0),dataArray.size());
      final int endIndex = Math.min(startIndex + Math.max(length,0),dataArray.size());

      final JsonArrayBuilder dataArrayBuilder = Json.createArrayBuilder();
      for (int i = startIndex; i < endIndex; i++) {
        dataArrayBuilder.add(dataArray.get(i));
      }
      final JsonObjectBuilder dataBuilder = Json.createObjectBuilder(resultAsJson.getJsonObject("data"));
      dataBuilder.add("draw",draw);
      dataBuilder.add("data",dataArrayBuilder.build());

      final JsonObjectBuilder responseBuilder = Json.createObjectBuilder(resultAsJson);
      responseBuilder.add("data",dataBuilder.build());
      return responseBuilder.build();
    }
    else if(json.getString("type").equals("uPlot") && type.equals(InterpreterResult.Type.UPLOT)){
      final JsonObject jsonOptions = json.getJsonObject("options");
      final String graphType = jsonOptions.containsKey("graphType") ? jsonOptions.getString("graphType") : "line";

      final JsonObject responseOptions = resultAsJson.getJsonObject("options");
      final JsonObjectBuilder responseOptionsBuilder = Json.createObjectBuilder(responseOptions);
      responseOptionsBuilder.add("graphType",graphType);
      final JsonObjectBuilder responseBuilder = Json.createObjectBuilder(resultAsJson);
      responseBuilder.add("options",responseOptionsBuilder.build());
      return responseBuilder.build();
    }
    else {
      throw new InterpreterException("Result does not match with given options!");
    }
  }

  @Override
  public JsonObject asJson() {
    // If the data within this resultMessage is in a JSON formatted type, perform the necessary formatting based on the information saved on the result.
    final JsonObjectBuilder json = Json.createObjectBuilder();
    if(type != null){
      json.add("type",type.label.toLowerCase());
    }
    if(data != null){
      try{
        JsonObject dataJson = Json.createReader(new StringReader(data)).readObject();
        json.add("data",dataJson);
      } catch (JsonParsingException e){
        // Encountered data, but it was not in JSON format. Non-JSON data is returned as a string.
        json.add("data",data);
      }
    }
    return json.build();
  }
}
