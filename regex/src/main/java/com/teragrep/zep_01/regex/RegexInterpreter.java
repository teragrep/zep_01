/*
 * Regex interpreter for Teragrep
 * Copyright (C) 2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.zep_01.regex;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import jakarta.json.*;
import jakarta.json.stream.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java interpreter
 */
public class RegexInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegexInterpreter.class);

  public RegexInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {


  }

  @Override
  public InterpreterResult interpret(String prompt, InterpreterContext context) {

    final InterpreterResult rv;

    int newlineIndex= prompt.indexOf('\n');
    if (newlineIndex != -1) {
      String regex = prompt.substring(0, newlineIndex);
      String content = prompt.substring(newlineIndex + 1);

      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(content);

      InterpreterResult rvLocal;
      try {
        // java 11 does not have namedGroups as public so reflection is needed
        Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");

        namedGroupsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Integer> groupMap = (Map<String, Integer>) namedGroupsMethod.invoke(pattern);


        if (matcher.find()) {
          JsonObjectBuilder builder = Json.createObjectBuilder();

          for (String key : groupMap.keySet()) {
            String value = matcher.group(key);

            if (value == null) {
              builder.addNull(key);
            } else {
              builder.add(key, value);
            }
          }

          JsonObject jsonObject = builder.build();

          Map<String, Boolean> config = Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true);
          JsonWriterFactory writerFactory = Json.createWriterFactory(config);

          StringWriter stringWriter = new StringWriter();
          try (JsonWriter jsonWriter = writerFactory.createWriter(stringWriter)) {
            jsonWriter.writeObject(jsonObject);
          }

          rvLocal = new InterpreterResult(InterpreterResult.Code.SUCCESS, stringWriter.toString());

        }
        else {
          rvLocal = new InterpreterResult(InterpreterResult.Code.ERROR, "regex \n\n" + regex + "\n\n does not match content\n\n" + content);
        }
      }
      catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        LOGGER.error("namedGroups reflection access error", e);
        rvLocal = new InterpreterResult(InterpreterResult.Code.ERROR, "namedGroups reflection access error");
      }
      rv = rvLocal;
    }
    else {
      rv = new InterpreterResult(InterpreterResult.Code.ERROR, "expected regex on the first line");
    }

    return rv;
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
                                                InterpreterContext interpreterContext) {
    return Collections.emptyList();
  }

}
