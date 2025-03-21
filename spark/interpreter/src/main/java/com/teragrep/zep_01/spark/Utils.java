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

package com.teragrep.zep_01.spark;

import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Utility and helper functions for the Spark Interpreter
 */
class Utils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static String buildJobGroupId(InterpreterContext context) {
    String uName = "anonymous";
    if (context.getAuthenticationInfo() != null) {
      uName = getUserName(context.getAuthenticationInfo());
    }
    return "zeppelin|" + uName + "|" + context.getNoteId() + "|" + context.getParagraphId();
  }

  public static String buildJobDesc(InterpreterContext context) {
    return "Started by: " + getUserName(context.getAuthenticationInfo());
  }

  public static String getUserName(AuthenticationInfo info) {
    String uName = "";
    if (info != null) {
      uName = info.getUser();
    }
    if (uName == null || uName.isEmpty()) {
      uName = "anonymous";
    }
    return uName;
  }

  public static void printDeprecateMessage(SparkVersion sparkVersion,
                                           InterpreterContext context,
                                           Properties properties) throws InterpreterException {
    context.out.clear();
    // print deprecated message only when zeppelin.spark.deprecatedMsg.show is true and
    // sparkVersion meets the certain requirements
  }
}
