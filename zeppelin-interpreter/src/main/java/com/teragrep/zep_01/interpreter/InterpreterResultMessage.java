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

  /**
   * This method is used for performing pagination and formatting requests on saved data when Interpreter is not running.
   * @param options Thrift union object containing a supported formatting type's options
   * @return JsonObject with required transformations based on given Options object
   * @throws InterpreterException If given an unsupported options type, or if InterpreterResultMessages data is malformed.
   */
  public JsonObject format(final Options options) throws InterpreterException {
    return format(Json.createReader(new StringReader(data)).readObject(), options);
  }

  private JsonObject format(JsonObject resultAsJson, Options options) throws InterpreterException {
    if(type == null){
      throw new InterpreterException("Result has no type assigned!");
    }
    if (options.isSetDataTablesOptions() && type.equals(InterpreterResult.Type.DATATABLES)){
      final DataTablesOptions dtOptions = options.getDataTablesOptions();
      final int draw = dtOptions.isSetDraw() ? dtOptions.getDraw() : 1;
      final int start = dtOptions.isSetStart() ? dtOptions.getStart() : 0;
      final int length = dtOptions.isSetLength() ? dtOptions.getLength() : 50;
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
    else if(options.isSetUPlotOptions() && type.equals(InterpreterResult.Type.UPLOT)){
      final UPlotOptions uPlotOptions = options.getUPlotOptions();
      final String graphType = uPlotOptions.isSetGraphType() ? uPlotOptions.getGraphType() : "line";

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
    final JsonObject json;
    if(type != null){
      try{
        if(type.equals(InterpreterResult.Type.DATATABLES)){
          DataTablesOptions options = new DataTablesOptions();
          JsonObject resultAsJson = Json.createReader(new StringReader(data)).readObject();
          if(resultAsJson.containsKey("data") && resultAsJson.get("data").getValueType().equals(JsonValue.ValueType.OBJECT)){
            JsonObject dataObject = resultAsJson.getJsonObject("data");
            if(dataObject.containsKey("draw") && dataObject.get("draw").getValueType().equals(JsonValue.ValueType.NUMBER)){
              options.setDraw(dataObject.getInt("draw"));
            }
          }
          json = format(resultAsJson, Options.dataTablesOptions(options));
        }
        else if(type.equals(InterpreterResult.Type.UPLOT)){
          UPlotOptions options = new UPlotOptions();
          JsonObject resultAsJson = Json.createReader(new StringReader(data)).readObject();
          if(resultAsJson.containsKey("options") && resultAsJson.get("options").getValueType().equals(JsonValue.ValueType.OBJECT)){
            JsonObject optionsObject = resultAsJson.getJsonObject("options");
            if(optionsObject.containsKey("graphType") && optionsObject.get("graphType").getValueType().equals(JsonValue.ValueType.STRING)){
              options.setGraphType(optionsObject.getString("graphType"));
            }
          }
          json = format(resultAsJson, Options.uPlotOptions(options));
        }
        else {
          // If the data is some other type, there is no guarantee that the data is even in JSON format, so we build a response assuming that data is a simple String.
          final JsonObjectBuilder resultBuilder = Json.createObjectBuilder();
          resultBuilder.add("data",data);
          resultBuilder.add("isAggregated",false);
          if(type != null){
            resultBuilder.add("type",type.label);
          }
          json = resultBuilder.build();
        }
      } catch (InterpreterException interpreterException){
        return JsonValue.EMPTY_JSON_OBJECT;
      }
    }
    else {
      json = JsonValue.EMPTY_JSON_OBJECT;
    }
    return json;
  }
}
