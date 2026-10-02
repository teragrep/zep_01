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

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceSet;
import org.apache.commons.text.StringSubstitutor;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import com.teragrep.zep_01.resource.ResourcePool;
import com.teragrep.zep_01.resource.ResourcePoolMap;

import java.util.*;
import java.util.stream.Stream;

public abstract class AbstractInterpreter extends Interpreter {
  public AbstractInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public InterpreterResult interpret(String st,
                                     InterpreterContext context) throws InterpreterException {
    InterpreterContext.set(context);
    ZeppelinContext z = getZeppelinContext();
    if (z != null) {
      z.setGui(context.getGui());
      z.setNoteGui(context.getNoteGui());
      z.setInterpreterContext(context);
    }
    boolean interpolate = isInterpolate() ||
            Boolean.parseBoolean(context.getLocalProperties().getOrDefault("interpolate", "false"));
    if (interpolate) {
      st = interpolate(st, context);
    }
    return internalInterpret(st, context);
  }

  static String interpolate(final String cmd, final InterpreterContext context){

    // Interpolation takes values either from ResourcePool or AngularObjectRegistry, prioritizing ResourcePool if duplicate keys are found.
    final String noteId = context.getNoteId();
    final String paragraphId = context.getParagraphId();
    final Map<String,Object> replacementMap = new HashMap<>();

    final AngularObjectRegistry registry = context.getAngularObjectRegistry();
    final ResourcePool resourcePool = context.getResourcePool();

    // Add all values from AngularObjectRegistry if it exists
    if(registry != null){
      final List<AngularObject> allObjects = new ArrayList<>();
      allObjects.addAll(registry.getAll(null,null));
      allObjects.addAll(registry.getAll(noteId,null));
      allObjects.addAll(registry.getAll(noteId,paragraphId));

      // Add each object to a Map from the List. Order of the List matters, value of a duplicated key in the Map will be the last occurrence of that key in the List.
      for (final AngularObject angularObject : allObjects) {
        replacementMap.put(angularObject.getName(),angularObject.get());
      }
    }

    // StringSubstitutor wants Map so convert ResourcePool if exists
    if(resourcePool != null){
      final ResourceSet resources = resourcePool.getAll();
      for (final Resource resource : resources) {
        replacementMap.put(resource.getResourceId().getName(), resource.get());
      }
    }

    final StringSubstitutor substitutor = new StringSubstitutor(replacementMap);
    // Recursive substitution is always disabled as that can lead to infinite recursion
    substitutor.setEnableSubstitutionInVariables(false);
    // Fail fast on undefined variables
    substitutor.setEnableUndefinedVariableException(true);
    // Try replacing, re-throw exception rewritten in more user-friendly way
    try {
      return substitutor.replace(cmd);
    }
    catch (final IllegalArgumentException e){
      throw new IllegalArgumentException(
              "Failure in interpolating variables. Ensure that all variables are resolvable or escape them as $${literal}.\n" +
                      "Original error message: " + e.getMessage()
      );
    }
  }

  public abstract ZeppelinContext getZeppelinContext();

  protected boolean isInterpolate() {
    return false;
  }

  protected abstract InterpreterResult internalInterpret(
          String st,
          InterpreterContext context) throws InterpreterException;

  @Override
  public List<InterpreterCompletion> completion(String buf,
                                                int cursor,
                                                InterpreterContext interpreterContext) throws InterpreterException {
    return new ArrayList<>();
  }
}
