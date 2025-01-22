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

package org.apache.zeppelin.display;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Base class for dynamic forms. Also used as factory class of dynamic forms.
 *
 * @param <T>
 */
public class Input<T> implements Serializable {

  protected String name;
  protected String displayName;
  protected T defaultValue;
  protected boolean hidden;
  protected String argument;

  public Input() {
  }

  public boolean isHidden() {
    return hidden;
  }

  public String getName() {
    return this.name;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public void setArgument(String argument) {
    this.argument = argument;
  }

  public void setHidden(boolean hidden) {
    this.hidden = hidden;
  }

  public String getArgument() {
    return argument;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Input<?> input = (Input<?>) o;

    if (hidden != input.hidden) {
      return false;
    }
    if (name != null ? !name.equals(input.name) : input.name != null) {
      return false;
    }
    if (displayName != null ? !displayName.equals(input.displayName) : input.displayName != null) {
      return false;
    }
    if (defaultValue instanceof Object[]) {
      if (defaultValue != null ?
          !Arrays.equals((Object[]) defaultValue, (Object[]) input.defaultValue)
          : input.defaultValue != null) {
        return false;
      }
    } else if (defaultValue != null ?
        !defaultValue.equals(input.defaultValue) : input.defaultValue != null) {
      return false;
    }
    return argument != null ? argument.equals(input.argument) : input.argument == null;

  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
    result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
    result = 31 * result + (hidden ? 1 : 0);
    result = 31 * result + (argument != null ? argument.hashCode() : 0);
    return result;
  }


  /*
   * public static String [] splitPipe(String str){ //return
   * str.split("\\|(?=([^\"']*\"[^\"']*\")*[^\"']*$)"); return
   * str.split("\\|(?=([^\"']*\"[^\"']*\")*[^\"']*$)"); }
   */

}
