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


package org.apache.zeppelin.display.ui;

import org.apache.zeppelin.display.InputImpl;
import com.teragrep.zep_04.display.ui.OptionInput;
import com.teragrep.zep_04.display.ui.ParamOption;

/**
 * Base class for Input with options
 *
 * @param <T>
 */
public abstract class OptionInputImpl<T> extends InputImpl<T> implements OptionInput<T> {

  protected ParamOption[] options;

  @Override
  public ParamOption[] getOptions() {
    return options;
  }
}
