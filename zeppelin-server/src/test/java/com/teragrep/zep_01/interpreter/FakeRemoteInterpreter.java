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

import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fake version of a RemoteInterpreter for use in testing
 */
public class FakeRemoteInterpreter extends Interpreter {
  AtomicReference<InterpreterContext> interpreterContext = new AtomicReference<>();
  public FakeRemoteInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    interpreterContext.set(context);
        try {
          // send 3 batches of data. Each containing the input string one additional time
          for (int i = 0; i < 3; i++) {
            Thread.sleep(500);
            JsonObjectBuilder output = Json.createObjectBuilder();
            output.add("data", i);
            interpreterContext.get().out().clear(false);
            interpreterContext.get().out().write("%" + InterpreterResult.Type.DATATABLES.label.toLowerCase() + "\n" + output.build().toString());
            interpreterContext.get().out().flush();
          }
        } catch (IOException | InterruptedException exception) {
          return new InterpreterResult(InterpreterResult.Code.ERROR, exception.getMessage());
        }
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, Json.createObjectBuilder().add("data", "").build().toString());
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public String formatDataset(final String noteId, final String paragraphId, final String options) throws InterpreterException {
    if(interpreterContext.get() != null){
      final String formatted = Json.createObjectBuilder().add("data", "consider yourself reformatted").add("type", InterpreterResult.Type.DATATABLES.label).build().toString();
      // this simulates DatasetStore calling saveToDisk(), which notifies NotebookServer that there is new output to display for the paragraph.
      try{
        Thread.sleep(500);
        interpreterContext.get().out().clear(false);
        interpreterContext.get().out().write("%" + InterpreterResult.Type.DATATABLES.label.toLowerCase() + "\n" + formatted.toString());
        interpreterContext.get().out().flush();
        return formatted;
      }
      catch (IOException | InterruptedException ioException){
        throw new InterpreterException(ioException);
      }
    }
    else {
      throw new InterpreterException("Attempt to format dataset without any call to interpret() first!");
    }
  }
}
