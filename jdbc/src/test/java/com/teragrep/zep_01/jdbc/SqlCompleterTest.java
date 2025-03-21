/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.teragrep.zep_01.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jline.console.completer.ArgumentCompleter;

import com.teragrep.zep_01.completer.CompletionType;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;

/**
 * SQL completer unit tests.
 */
class SqlCompleterTest {
  public class CompleterTester {
    private SqlCompleter completer;

    private String buffer;
    private int fromCursor;
    private int toCursor;
    private Set<InterpreterCompletion> expectedCompletions;

    CompleterTester(SqlCompleter completer) {
      this.completer = completer;
    }

    public CompleterTester buffer(String buffer) {
      this.buffer = buffer;
      return this;
    }

    public CompleterTester from(int fromCursor) {
      this.fromCursor = fromCursor;
      return this;
    }

    public CompleterTester to(int toCursor) {
      this.toCursor = toCursor;
      return this;
    }

    public CompleterTester expect(Set<InterpreterCompletion> expectedCompletions) {
      this.expectedCompletions = expectedCompletions;
      return this;
    }

    public void test() {
      for (int c = fromCursor; c <= toCursor; c++) {
        expectedCompletions(buffer, c, expectedCompletions);
      }
    }

    private void expectedCompletions(String buffer, int cursor,
        Set<InterpreterCompletion> expected) {
      if (StringUtils.isNotEmpty(buffer) && buffer.length() > cursor) {
        buffer = buffer.substring(0, cursor);
      }

      List<InterpreterCompletion> candidates = new ArrayList<>();

      completer.complete(buffer, cursor, candidates);

      String explain = explain(buffer, cursor, candidates);

      logger.debug(explain);

      assertEquals(expected, new HashSet<>(candidates), "Buffer [" + buffer.replace(" ", ".")
              + "] and Cursor[" + cursor + "] " + explain);
    }

    private String explain(String buffer, int cursor, List<InterpreterCompletion> candidates) {
      List<String> cndidateStrings = new ArrayList<>();
      for (InterpreterCompletion candidate : candidates) {
        cndidateStrings.add(candidate.getValue());
      }
      StringBuffer sb = new StringBuffer();

      for (int i = 0; i <= Math.max(cursor, buffer.length()); i++) {
        if (i == cursor) {
          sb.append("(");
        }
        if (i >= buffer.length()) {
          sb.append("_");
        } else {
          if (Character.isWhitespace(buffer.charAt(i))) {
            sb.append(".");
          } else {
            sb.append(buffer.charAt(i));
          }
        }
        if (i == cursor) {
          sb.append(")");
        }
      }
      sb.append(" >> [").append(String.join(",", cndidateStrings)).append("]");

      return sb.toString();
    }
  }

  private Logger logger = LoggerFactory.getLogger(SqlCompleterTest.class);

  private CompleterTester tester;

  private ArgumentCompleter.WhitespaceArgumentDelimiter delimiter =
      new ArgumentCompleter.WhitespaceArgumentDelimiter();

  private SqlCompleter sqlCompleter = new SqlCompleter(0);

  @BeforeEach
  public void beforeTest() throws IOException, SQLException {
    Set<String> schemas = new HashSet<>();
    Set<String> keywords = new HashSet<>();

    keywords.add("SUM");
    keywords.add("SUBSTRING");
    keywords.add("SUBCLASS_ORIGIN");
    keywords.add("ORDER");
    keywords.add("SELECT");
    keywords.add("LIMIT");
    keywords.add("FROM");

    sqlCompleter.initKeywords(keywords);

    schemas.add("prod_dds");
    schemas.add("prod_emart");

    sqlCompleter.initSchemas(schemas);

    Set<String> prodDdsTables = new HashSet<>();
    prodDdsTables.add("financial_account");
    prodDdsTables.add("customer");

    sqlCompleter.initTables("prod_dds", prodDdsTables);

    Set<String> prodEmartTables = new HashSet<>();
    prodEmartTables.add("financial_account");

    sqlCompleter.initTables("prod_emart", prodEmartTables);

    Set<String> prodDdsFinancialAccountColumns = new HashSet<>();
    prodDdsFinancialAccountColumns.add("account_rk");
    prodDdsFinancialAccountColumns.add("account_id");

    sqlCompleter.initColumns("prod_dds.financial_account", prodDdsFinancialAccountColumns);

    Set<String> prodDdsCustomerColumns = new HashSet<>();
    prodDdsCustomerColumns.add("customer_rk");
    prodDdsCustomerColumns.add("name");
    prodDdsCustomerColumns.add("birth_dt");

    sqlCompleter.initColumns("prod_dds.customer", prodDdsCustomerColumns);

    Set<String> prodEmartFinancialAccountColumns = new HashSet<>();
    prodEmartFinancialAccountColumns.add("account_rk");
    prodEmartFinancialAccountColumns.add("balance_amt");

    sqlCompleter.initColumns("prod_emart.financial_account", prodEmartFinancialAccountColumns);

    tester = new CompleterTester(sqlCompleter);
  }

  @Test
  void testFindAliasesInSQL_Simple() {
    String sql = "select * from prod_emart.financial_account a";
    Map<String, String> res = sqlCompleter.findAliasesInSQL(
            delimiter.delimit(sql, 0).getArguments());
    assertEquals(1, res.size());
    assertEquals("prod_emart.financial_account", res.get("a"));
  }

  @Test
  void testFindAliasesInSQL_Two() {
    String sql = "select * from prod_dds.financial_account a, prod_dds.customer b";
    Map<String, String> res = sqlCompleter.findAliasesInSQL(
            sqlCompleter.getSqlDelimiter().delimit(sql, 0).getArguments());
    assertEquals(2, res.size());
    assertEquals("prod_dds.financial_account", res.get("a"));
    assertEquals("prod_dds.customer", res.get("b"));
  }

  @Test
  void testFindAliasesInSQL_WrongTables() {
    String sql = "select * from prod_ddsxx.financial_account a, prod_dds.customerxx b";
    Map<String, String> res = sqlCompleter.findAliasesInSQL(
            sqlCompleter.getSqlDelimiter().delimit(sql, 0).getArguments());
    assertEquals(0, res.size());
  }

  @Test
  void testCompleteName_Empty() {
    String buffer = "";
    int cursor = 0;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(9, candidates.size());
    assertTrue(candidates.contains(new InterpreterCompletion("prod_dds", "prod_dds",
            CompletionType.schema.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("prod_emart", "prod_emart",
            CompletionType.schema.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("SUM", "SUM",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("SUBSTRING", "SUBSTRING",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("SUBCLASS_ORIGIN", "SUBCLASS_ORIGIN",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("SELECT", "SELECT",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("ORDER", "ORDER",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("LIMIT", "LIMIT",
            CompletionType.keyword.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("FROM", "FROM",
            CompletionType.keyword.name())));
  }

  @Test
  void testCompleteName_SimpleSchema() {
    String buffer = "prod_";
    int cursor = 3;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(2, candidates.size());
    assertTrue(candidates.contains(new InterpreterCompletion("prod_dds", "prod_dds",
            CompletionType.schema.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("prod_emart", "prod_emart",
            CompletionType.schema.name())));
  }

  @Test
  void testCompleteName_SimpleTable() {
    String buffer = "prod_dds.fin";
    int cursor = 11;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(1, candidates.size());
    assertTrue(candidates.contains(
            new InterpreterCompletion("financial_account", "financial_account",
                    CompletionType.table.name())));
  }

  @Test
  void testCompleteName_SimpleColumn() {
    String buffer = "prod_dds.financial_account.acc";
    int cursor = 30;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(2, candidates.size());
    assertTrue(candidates.contains(new InterpreterCompletion("account_rk", "account_rk",
            CompletionType.column.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("account_id", "account_id",
            CompletionType.column.name())));
  }

  @Test
  void testCompleteName_WithAlias() {
    String buffer = "a.acc";
    int cursor = 4;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    aliases.put("a", "prod_dds.financial_account");
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(2, candidates.size());
    assertTrue(candidates.contains(new InterpreterCompletion("account_rk", "account_rk",
            CompletionType.column.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("account_id", "account_id",
            CompletionType.column.name())));
  }

  @Test
  void testCompleteName_WithAliasAndPoint() {
    String buffer = "a.";
    int cursor = 2;
    List<InterpreterCompletion> candidates = new ArrayList<>();
    Map<String, String> aliases = new HashMap<>();
    aliases.put("a", "prod_dds.financial_account");
    sqlCompleter.completeName(buffer, cursor, candidates, aliases);
    assertEquals(2, candidates.size());
    assertTrue(candidates.contains(new InterpreterCompletion("account_rk", "account_rk",
            CompletionType.column.name())));
    assertTrue(candidates.contains(new InterpreterCompletion("account_id", "account_id",
            CompletionType.column.name())));
  }

  @Test
  void testSchemaAndTable() {
    String buffer = "select * from prod_emart.fi";
    tester.buffer(buffer).from(20).to(23).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("prod_emart", "prod_emart",
                    CompletionType.schema.name())))).test();
    tester.buffer(buffer).from(25).to(27).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("financial_account", "financial_account",
                    CompletionType.table.name())))).test();
  }

  @Test
  void testEdges() {
    String buffer = "  ORDER  ";
    tester.buffer(buffer).from(3).to(7).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("ORDER", "ORDER", CompletionType.keyword.name())))).test();
    tester.buffer(buffer).from(0).to(1).expect(new HashSet<>(Arrays.asList(
        new InterpreterCompletion("ORDER", "ORDER", CompletionType.keyword.name()),
        new InterpreterCompletion("SUBCLASS_ORIGIN", "SUBCLASS_ORIGIN",
                CompletionType.keyword.name()),
        new InterpreterCompletion("SUBSTRING", "SUBSTRING", CompletionType.keyword.name()),
        new InterpreterCompletion("prod_emart", "prod_emart", CompletionType.schema.name()),
        new InterpreterCompletion("LIMIT", "LIMIT", CompletionType.keyword.name()),
        new InterpreterCompletion("SUM", "SUM", CompletionType.keyword.name()),
        new InterpreterCompletion("prod_dds", "prod_dds", CompletionType.schema.name()),
        new InterpreterCompletion("SELECT", "SELECT", CompletionType.keyword.name()),
        new InterpreterCompletion("FROM", "FROM", CompletionType.keyword.name())
    ))).test();
  }

  @Test
  void testMultipleWords() {
    String buffer = "SELE FRO LIM";
    tester.buffer(buffer).from(2).to(4).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("SELECT", "SELECT", CompletionType.keyword.name())))).test();
    tester.buffer(buffer).from(6).to(8).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("FROM", "FROM", CompletionType.keyword.name())))).test();
    tester.buffer(buffer).from(10).to(12).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("LIMIT", "LIMIT", CompletionType.keyword.name())))).test();
  }

  @Test
  void testMultiLineBuffer() {
    String buffer = " \n SELE\nFRO";
    tester.buffer(buffer).from(5).to(7).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("SELECT", "SELECT", CompletionType.keyword.name())))).test();
    tester.buffer(buffer).from(9).to(11).expect(new HashSet<>(Arrays.asList(
            new InterpreterCompletion("FROM", "FROM", CompletionType.keyword.name())))).test();
  }

  @Test
  void testMultipleCompletionSuggestions() {
    String buffer = "SU";
    tester.buffer(buffer).from(2).to(2).expect(new HashSet<>(Arrays.asList(
        new InterpreterCompletion("SUBCLASS_ORIGIN", "SUBCLASS_ORIGIN",
                CompletionType.keyword.name()),
        new InterpreterCompletion("SUM", "SUM", CompletionType.keyword.name()),
        new InterpreterCompletion("SUBSTRING", "SUBSTRING", CompletionType.keyword.name()))
    )).test();
  }

  @Test
  void testSqlDelimiterCharacters() {
    assertTrue(sqlCompleter.getSqlDelimiter().isDelimiterChar("r,", 1));
    assertTrue(sqlCompleter.getSqlDelimiter().isDelimiterChar("SS,", 2));
    assertTrue(sqlCompleter.getSqlDelimiter().isDelimiterChar(",", 0));
    assertTrue(sqlCompleter.getSqlDelimiter().isDelimiterChar("ttt,", 3));
  }
}
