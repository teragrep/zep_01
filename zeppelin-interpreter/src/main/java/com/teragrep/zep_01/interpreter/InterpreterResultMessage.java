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
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.Options;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
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

  /**
   * This method is used for performing pagination and formatting requests on saved data when Interpreter is not running.
   * @param options Thrift union object containing a supported formatting type's options
   * @return JsonObject with required transformations based on given Options object
   * @throws JsonException If given an unsupported options type, or if InterpreterResultMessages data is malformed.
   */
  public JsonObject asJsonwithOptions(Options options) throws JsonException{
    if(type == null){
      throw new JsonException("Result has no type assigned!");
    }
    if (options.isSetDataTablesOptions() && type.equals(InterpreterResult.Type.DATATABLES)){
      DataTablesOptions dtOptions = options.getDataTablesOptions();
      int draw = dtOptions.getDraw();
      int start = dtOptions.getStart();
      int length = dtOptions.getLength();
      JsonObject resultAsJson = asJson();
      JsonObject data = resultAsJson.getJsonObject("data");
      JsonArray dataArray = data.getJsonArray("data");

      int startIndex = Math.min(Math.max(start,0),dataArray.size());
      int endIndex = Math.min(startIndex + Math.max(length,0),dataArray.size());

      JsonArrayBuilder dataArrayBuilder = Json.createArrayBuilder();
      for (int i = startIndex; i < endIndex; i++) {
        dataArrayBuilder.add(dataArray.get(i));
      }
      JsonObjectBuilder dataBuilder = Json.createObjectBuilder(resultAsJson.getJsonObject("data"));
      dataBuilder.add("draw",draw);
      dataBuilder.add("data",dataArrayBuilder.build());

      JsonObjectBuilder responseBuilder = Json.createObjectBuilder(resultAsJson);
      responseBuilder.add("data",dataBuilder.build());
      return responseBuilder.build();
    }
    else if(options.isSetUPlotOptions() && type.equals(InterpreterResult.Type.UPLOT)){
      UPlotOptions uPlotOptions = options.getUPlotOptions();
      String graphType = uPlotOptions.getGraphType();

      JsonObject resultAsJson = asJson();
      JsonObject responseOptions = resultAsJson.getJsonObject("options");
      JsonObjectBuilder responseOptionsBuilder = Json.createObjectBuilder(responseOptions);
      responseOptionsBuilder.add("graphType",graphType);
      JsonObjectBuilder responseBuilder = Json.createObjectBuilder(resultAsJson);
      responseBuilder.add("options",responseOptionsBuilder.build());
      return responseBuilder.build();
    }
    else {
      throw new JsonException("Result does not match with given options!");
    }
  }

  @Override
  public JsonObject asJson() {
    // If the data within this resultMessage is in a JSON formatted type, simply return the data object itself.
    if(type != null && (type.equals(InterpreterResult.Type.DATATABLES) || type.equals(InterpreterResult.Type.UPLOT))){
      return Json.createReader(new StringReader(data)).readObject();
    }
    // If the data is some other type, there is no guarantee that the data is even in JSON format, so we build a response assuming that data is a simple String.
    else {
      final JsonObjectBuilder resultBuilder = Json.createObjectBuilder();
      resultBuilder.add("data",data);
      resultBuilder.add("isAggregated",false);
      if(type != null){
        resultBuilder.add("type",type.label);
      }
      return resultBuilder.build();
    }
  }
}
