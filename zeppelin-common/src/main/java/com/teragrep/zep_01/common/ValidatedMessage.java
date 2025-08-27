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

import com.google.gson.internal.LinkedTreeMap;
import jakarta.json.*;

import java.util.Objects;

public final class ValidatedMessage {
  private final Message messageToValidate;

  /**
   * Checks that the message provided contains all of the required keys for a PARAGRAPH_UPDATE_RESULT message. Returns true if message is valid, false if not.
   * @param messageToValidate Message object to be validated
   */
  public ValidatedMessage(Message messageToValidate){
    this.messageToValidate = messageToValidate;
  }
  public boolean isValid() throws JsonException {
    boolean valid = true;
    if(! messageToValidate.op.equals(Message.OP.PARAGRAPH_UPDATE_RESULT)){
      valid = false;
    }
    if(valid && (! messageToValidate.data.containsKey("noteId") || ! messageToValidate.data.get("noteId").getClass().equals(String.class))){
      valid = false;
    }
    if(valid && (! messageToValidate.data.containsKey("paragraphId") || ! messageToValidate.data.get("paragraphId").getClass().equals(String.class))){
      valid = false;
    }
    // GSON parses all numbers as Double, regardless of if they are integers or floats in the source JSON.
    if(valid && (! messageToValidate.data.containsKey("start") || ! messageToValidate.data.get("start").getClass().equals(Double.class))){
      valid = false;
    }
    // GSON parses all numbers as Double, regardless of if they are integers or floats in the source JSON.
    if(valid && (! messageToValidate.data.containsKey("length") || ! messageToValidate.data.get("length").getClass().equals(Double.class))){
      valid = false;
    }
    // GSON parses all numbers as Double, regardless of if they are integers or floats in the source JSON.
    if(valid && (! messageToValidate.data.containsKey("draw") || ! messageToValidate.data.get("draw").getClass().equals(Double.class))){
      valid = false;
    }
    // GSON parses all JSON objects as LinkedTreeMaps
    if(valid && (! messageToValidate.data.containsKey("search") || ! messageToValidate.data.get("search").getClass().equals(LinkedTreeMap.class))) {
      valid = false;
    }
    // GSON parses all JSON objects as LinkedTreeMaps
    LinkedTreeMap<String,Object> searchMap = (LinkedTreeMap<String, java.lang.Object>) messageToValidate.data.get("search");
      if(valid && (! searchMap.containsKey("value") || ! searchMap.get("value").getClass().equals(String.class))){
        valid = false;
      }
    return valid;
  }

  // Objects are equal if they are applied to the same Message object
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidatedMessage that = (ValidatedMessage) o;
    return Objects.equals(messageToValidate, that.messageToValidate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(messageToValidate);
  }
}
