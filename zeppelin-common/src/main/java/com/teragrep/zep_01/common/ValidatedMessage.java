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

import jakarta.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.*;

/**
 * Decorator for Message that can validate its parameters by comparing to a provided JsonObject.
 * The message is considered valid when the underlying Message's data object contains every key that is present in the same position in the hierarchy as in provided JsonObject.
 * For example, {"search":{"value":"searchTerm","regex":false"}} matches with
 *              {"search":{"regex":""}} but not with {"search:"","regex":""}
 */
public class ValidatedMessage {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidatedMessage.class);
  private final Message origin;
  private final JsonObject requiredParameters;

  public ValidatedMessage(Message origin, JsonObject requiredParameters){
    this.origin = origin;
    this.requiredParameters = requiredParameters;
  }

  public Message withMsgId(String msgId) {
    return origin.withMsgId(msgId);
  }

  public Message put(String k, Object v) {
    return origin.put(k, v);
  }

  public Object get(String k) {
    return origin.get(k);
  }

  public static boolean isDisabledForRunningNotes(Message.OP eventType) {
    return Message.isDisabledForRunningNotes(eventType);
  }

  public <T> T getType(String key) {
    return origin.getType(key);
  }

  public <T> T getType(String key, Logger log) {
    return origin.getType(key, log);
  }

  @Override
  public String toString() {
    return origin.toString();
  }

  public String toJson() {
    return origin.toJson();
  }

  public static Message fromJson(String json) {
    return Message.fromJson(json);
  }
  public boolean validate(){
    try{
      validate(Json.createReader(new StringReader(origin.toJson())).readObject(),requiredParameters);
      return true;
    } catch (JsonException jsonException){
      LOGGER.warn("Validation of Message failed: ",jsonException);
      return false;
    }
  }

  /**
   *
   * @param toValidate
   * @param toCompare
   * @return
   * @throws JsonException
   */
  public boolean validate(JsonValue toValidate, JsonValue toCompare) throws JsonException {
    // The Json we are validating must be the same type as the Json we have saved for comparison, otherwise they do not match
    // If at any point in the recursion a JsonException is thrown, the message is not valid.

    JsonValue.ValueType typeToValidate = toValidate.getValueType();
    JsonValue.ValueType typeToCompare = toCompare.getValueType();
    // We only check the keys of JsonObjects and JsonArrays, values of primitives are ignored.
    if(typeToValidate.equals(JsonValue.ValueType.OBJECT) || typeToValidate.equals(JsonValue.ValueType.ARRAY)){
      if(typeToValidate.equals(typeToCompare)){
        // If the JsonValue to validate is a JSONObject, call validateObject on it
        if(typeToValidate.equals(JsonValue.ValueType.OBJECT)){
          JsonObject objectToValidate = toValidate.asJsonObject();
          JsonObject objectToCompare = toCompare.asJsonObject();
          validateObject(objectToValidate,objectToCompare);
        }

        // If the JsonValue to validate is a JSONOArray, call validateArray on it
        if(typeToValidate.equals(JsonValue.ValueType.ARRAY)){
          JsonArray jsonArrayToValidate = toValidate.asJsonArray();
          JsonArray requiredArray = toCompare.asJsonArray();
          validateArray(jsonArrayToValidate,requiredArray);
        }
        // You could add a check here if you want to validate the value as well.
      }
      else {
        throw new JsonException("Given JsonValue's type \""+typeToValidate+"\" is not of the correct type \"" +typeToCompare+"\"");
      }
    }
    // If we succesfully iterated over every required key without throwing any Exceptions, the message is valid.
    return true;
  }

  /**
   *
   * @param toValidate
   * @param toCompare
   * @throws JsonException
   */
  private void validateObject(JsonObject toValidate, JsonObject toCompare) throws JsonException{
    for (String requiredKey : toCompare.keySet()) {
      if(!toValidate.containsKey(requiredKey)){
        throw new JsonException("Provided JSONObject does not contain a required key "+requiredKey);
      }
      JsonValue valueToValidate = toValidate.get(requiredKey);
      JsonValue valueToCompare = toCompare.get(requiredKey);
      validate(valueToValidate, valueToCompare);
    }
  }

  /**
   *
   * @param toValidate
   * @param toCompare
   * @throws JsonException
   */
  private void validateArray(JsonArray toValidate, JsonArray toCompare) throws JsonException{
    List<JsonValue> valuesToValidate = toValidate.getValuesAs(JsonValue.class);
    List<JsonValue> valuesToCompare = toCompare.getValuesAs(JsonValue.class);
    // Loop through every value we expect to exist in the array, and throw an Exception if we did not find a match for each one.
      for (JsonValue valueToCompare : valuesToCompare) {
        boolean matchFound = false;
        for (JsonValue valueToValidate : valuesToValidate) {
          try{
            validate(valueToValidate,valueToCompare);
            matchFound = true;
            break;
            // If validate does not throw an Exception, it has found a match, and we can break to move on to check the next required value.
          } catch (JsonException jsonException){
            // If validate throws an Exception, it didn't find a match, but a match may still be found in one of the remaining values in the array
          }
        }
        if(!matchFound){
          throw new JsonException("JsonArray does not contain a required key");
        }
      }
    }
}
