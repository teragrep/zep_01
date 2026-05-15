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
import java.util.regex.PatternSyntaxException;
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
    try {
      LOGGER.info("Interpreting prompt <[{}]>", prompt);

      int newlineIndex = prompt.indexOf('\n');

      if (newlineIndex == -1) {
        throw new RegexInterpreterException("unrecognized prompt, please use regex on the first line and content on subsequent line(s)");
      }

      String regex = prompt.substring(0, newlineIndex);
      LOGGER.info("Extracted regex <[{}]>", regex);
      String content = prompt.substring(newlineIndex + 1);
      LOGGER.info("Extracted content <[{}]>", content);

      final Pattern pattern;
      try {
        pattern = Pattern.compile(regex);
      }
      catch (PatternSyntaxException e) {
        LOGGER.error("regex compilation failed", e);
        throw new RegexInterpreterException("regex compilation failed", e);
      }

      Matcher matcher = pattern.matcher(content);

      final Method namedGroupsMethod;
      try {
        // java 11 does not have namedGroups as public so reflection is needed
        namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
      }
      catch (NoSuchMethodException e) {
        LOGGER.error("reflection error getDeclaredMethod", e);
        throw new RegexInterpreterException("reflection error getDeclaredMethod", e);
      }

      namedGroupsMethod.setAccessible(true);

      final Map<String, Integer> groupMap;
      try {
        @SuppressWarnings("unchecked")
        final Map<String, Integer> groupMapLocal = (Map<String, Integer>) namedGroupsMethod.invoke(pattern);
        groupMap = groupMapLocal;
      }
      catch (InvocationTargetException | IllegalAccessException e) {
        LOGGER.error("reflection error invoke", e);
        throw new RegexInterpreterException("reflection error invoke", e);
      }

      if (!matcher.matches()) {
        LOGGER.warn("regex does not match content");
        throw new RegexInterpreterException("Provided regex\n----\n" + regex + "\n----\nDoes not match provided content\n----\n" + content + "\n----");
      }

      final JsonObjectBuilder builder = Json.createObjectBuilder();

      for (String key : groupMap.keySet()) {

        final String value = matcher.group(key);

        if (value == null) {
          builder.addNull(key);
        }
        else {
          builder.add(key, value);
        }
      }

      final JsonObject jsonObject = builder.build();

      final Map<String, Boolean> config = Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true);

      final JsonWriterFactory writerFactory = Json.createWriterFactory(config);

      final StringWriter stringWriter = new StringWriter();
      try (JsonWriter jsonWriter = writerFactory.createWriter(stringWriter)) {
        jsonWriter.writeObject(jsonObject);
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, stringWriter.toString());
    }
    catch (RegexInterpreterException rie) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, rie.getMessage());
    }
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
