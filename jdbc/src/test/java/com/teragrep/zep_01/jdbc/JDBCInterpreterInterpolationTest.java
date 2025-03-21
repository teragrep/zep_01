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

import com.mockrunner.jdbc.BasicJDBCTestCaseAdapter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import com.teragrep.zep_01.resource.LocalResourcePool;
import com.teragrep.zep_01.resource.ResourcePool;
import com.teragrep.zep_01.user.AuthenticationInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * JDBC interpreter Z-variable interpolation unit tests.
 */
@Disabled(value="Uses old interpolation")
class JDBCInterpreterInterpolationTest extends BasicJDBCTestCaseAdapter {

  private static String jdbcConnection;
  private InterpreterContext interpreterContext;
  private ResourcePool resourcePool;

  private String getJdbcConnection() throws IOException {
    if (null == jdbcConnection) {
      Path tmpDir = new File("target/h2-test").toPath().toAbsolutePath();
      jdbcConnection = format("jdbc:h2:%s", tmpDir);
    }
    return jdbcConnection;
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    Class.forName("org.h2.Driver");
    Connection connection = DriverManager.getConnection(getJdbcConnection());
    Statement statement = connection.createStatement();
    statement.execute(
          "DROP TABLE IF EXISTS test_table; " +
          "CREATE TABLE test_table(id varchar(255), name varchar(255));");

    Statement insertStatement = connection.createStatement();
    insertStatement.execute("insert into test_table(id, name) values " +
                "('pro', 'processor')," +
                "('mem', 'memory')," +
                "('key', 'keyboard')," +
                "('mou', 'mouse');");
    resourcePool = new LocalResourcePool("JdbcInterpolationTest");

    interpreterContext = getInterpreterContext();
  }

  @Test
  void testEnableDisableProperty() throws IOException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.h2.Driver");
    properties.setProperty("default.url", getJdbcConnection());
    properties.setProperty("default.user", "");
    properties.setProperty("default.password", "");

    resourcePool.put("zid", "mem");
    String sqlQuery = "select * from test_table where id = '${zid}'";

    //
    // Empty result expected because "zeppelin.jdbc.interpolation" is false by default ...
    //
    JDBCInterpreter t = new JDBCInterpreter(properties);
    t.open();
    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());

    List<InterpreterResultMessage> resultMessages =
            interpreterContext.out.toInterpreterResultMessage();
    assertEquals(1, resultMessages.size());
    assertEquals(InterpreterResult.Type.TABLE, resultMessages.get(0).getType());
    assertEquals("ID\tNAME\n", resultMessages.get(0).getData());

    //
    // 1 result expected because "zeppelin.jdbc.interpolation" set to "true" ...
    //
    properties.setProperty("zeppelin.jdbc.interpolation", "true");
    t = new JDBCInterpreter(properties);
    t.open();
    interpreterContext = getInterpreterContext();
    interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());

    resultMessages = interpreterContext.out.toInterpreterResultMessage();
    assertEquals(1, resultMessages.size());
    assertEquals(InterpreterResult.Type.TABLE, resultMessages.get(0).getType());
    assertEquals("ID\tNAME\nmem\tmemory\n", resultMessages.get(0).getData());
  }

  @Test
  @Disabled(value="Empty interpolation will fail loudly instead of passing emptiness silently")
  void testNormalQueryInterpolation() throws IOException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.h2.Driver");
    properties.setProperty("default.url", getJdbcConnection());
    properties.setProperty("default.user", "");
    properties.setProperty("default.password", "");

    properties.setProperty("zeppelin.jdbc.interpolation", "true");

    JDBCInterpreter t = new JDBCInterpreter(properties);
    t.open();

    //
    // Empty result expected because "kbd" is not defined ...
    //
    String sqlQuery = "select * from test_table where id = '{kbd}'";
    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());

    List<InterpreterResultMessage> resultMessages =
            interpreterContext.out.toInterpreterResultMessage();
    assertEquals(1, resultMessages.size());
    assertEquals(InterpreterResult.Type.TABLE, resultMessages.get(0).getType());
    assertEquals("ID\tNAME\n", resultMessages.get(0).getData());

    resourcePool.put("itemId", "key");

    //
    // 1 result expected because z-variable 'item' is 'key' ...
    //
    sqlQuery = "select * from test_table where id = '{itemId}'";
    interpreterContext = getInterpreterContext();
    interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    resultMessages = interpreterContext.out.toInterpreterResultMessage();
    assertEquals(1, resultMessages.size());
    assertEquals(InterpreterResult.Type.TABLE, resultMessages.get(0).getType());
    assertEquals("ID\tNAME\nkey\tkeyboard\n", resultMessages.get(0).getData());
  }

  @Test
  void testEscapedInterpolationPattern() throws IOException, InterpreterException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.h2.Driver");
    properties.setProperty("default.url", getJdbcConnection());
    properties.setProperty("default.user", "");
    properties.setProperty("default.password", "");

    properties.setProperty("zeppelin.jdbc.interpolation", "true");

    JDBCInterpreter t = new JDBCInterpreter(properties);
    t.open();

    //
    // 2 rows (keyboard and mouse) expected when searching names with 2 consecutive vowels ...
    // The 'regexp' keyword is specific to H2 database
    //
    String sqlQuery = "select * from test_table where name regexp '[aeiou]{2}'";
    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    List<InterpreterResultMessage> resultMessages =
            interpreterContext.out.toInterpreterResultMessage();
    assertEquals(1, resultMessages.size());
    assertEquals(InterpreterResult.Type.TABLE, resultMessages.get(0).getType());
    assertEquals("ID\tNAME\nkey\tkeyboard\nmou\tmouse\n", resultMessages.get(0).getData());
  }

  private InterpreterContext getInterpreterContext() {
    return InterpreterContext.builder()
            .setParagraphId("paragraph_1")
            .setAuthenticationInfo(new AuthenticationInfo("testUser"))
            .setResourcePool(resourcePool)
            .setInterpreterOut(new InterpreterOutput())
            .build();
  }
}
