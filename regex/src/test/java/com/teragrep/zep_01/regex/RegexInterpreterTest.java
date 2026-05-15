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

import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.regex.RegexInterpreter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class RegexInterpreterTest {

  private static RegexInterpreter java;
  private static InterpreterContext context;

  @BeforeClass
  public static void setUp() {
    Properties p = new Properties();
    regexInterpreter = new RegexInterpreter(p);
    regexInterpreter.open();
    context = InterpreterContext.builder().build();
  }

  @AfterClass
  public static void tearDown() {
    regexInterpreter.close();
  }

  @Test
  public void testStaticRepl() {

    StringWriter writer = new StringWriter();
    PrintWriter out = new PrintWriter(writer);
    out.println("^(?<line>.*)$");
    out.println("content that matches really well to this regex");
    out.close();

    InterpreterResult res = regexInterpreter.interpret(writer.toString(), context);

    assertEquals(InterpreterResult.Code.SUCCESS, res.code());
    assertEquals(InterpreterResult.Type.TEXT, res.message().get(0).getType());
  }


  @Test
  public void testEmpty() {
    InterpreterResult res = java.interpret("", context);

    assertEquals(InterpreterResult.Code.ERROR, res.code());
    assertEquals("expected regex on the first line", res.message().get(0));

  }

}
