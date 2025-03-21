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


package com.teragrep.zep_01.display.ui;

import com.teragrep.zep_01.display.Input;

/**
 * Base class for Input with options
 *
 * @param <T>
 */
public abstract class OptionInput<T> extends Input<T> {

  /**
   * Parameters option.
   */
  public static class ParamOption {
    Object value;
    String displayName;

    public ParamOption(Object value, String displayName) {
      super();
      this.value = value;
      this.displayName = displayName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ParamOption that = (ParamOption) o;

      if (value != null ? !value.equals(that.value) : that.value != null) return false;
      return displayName != null ? displayName.equals(that.displayName) : that.displayName == null;

    }

    @Override
    public int hashCode() {
      int result = value != null ? value.hashCode() : 0;
      result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
      return result;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }

  }

  protected ParamOption[] options;

  public ParamOption[] getOptions() {
    return options;
  }
}
