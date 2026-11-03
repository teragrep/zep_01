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

import java.util.*;
import java.util.regex.Pattern;

import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import com.teragrep.zep_01.regex.captureGroup.*;

/**
 * Java interpreter
 */
public class RegexInterpreter extends Interpreter {

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
      SkippablePrompt skippablePrompt = new SkippablePrompt(prompt);
      SplitablePrompt splitablePrompt = new SplitablePrompt(skippablePrompt.skipFirstLine());

      String regex = splitablePrompt.regex();

      RegexString regexString = new RegexString(regex);
      Pattern pattern = regexString.asPattern();

      NamedGroupsPattern namedGroupsPattern = new NamedGroupsPattern(pattern);

      String content = splitablePrompt.content();
      MatchableContent matchableContent = new MatchableContent(namedGroupsPattern, content);

      List<Group> groups = matchableContent.captureGroups();


      final List<Jsonable>  jsonableGroups = new ArrayList<>();
      for (Group group : groups) {
        if (group.name().isStub()) {
          jsonableGroups.add(new GroupJsonableImpl(group));
        }
        else {
          jsonableGroups.add(new DescribedGroupJsonable(new DescribedGroupImpl(group)));
        }
      }

      Output output = new Output(regex, jsonableGroups);

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, output.toString());
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
