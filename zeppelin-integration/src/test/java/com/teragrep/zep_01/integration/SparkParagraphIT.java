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

package com.teragrep.zep_01.integration;


import com.teragrep.zep_01.AbstractZeppelinIT;
import com.teragrep.zep_01.ZeppelinITUtils;
import com.teragrep.zep_01.WebDriverManager;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SparkParagraphIT extends AbstractZeppelinIT {
  private static final Logger LOG = LoggerFactory.getLogger(SparkParagraphIT.class);

  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @Before
  public void startUp() {
    driver = WebDriverManager.getWebDriver();
    createNewNote();
    waitForParagraph(1, "READY");
  }

  @After
  public void tearDown() {
    deleteTestNotebook(driver);
    driver.quit();
  }

  @Ignore(value="Contains UI specific values")
  @Test
  public void testSpark() throws Exception {
    try {
      setTextOfParagraph(1, "sc.version");
      runParagraph(1);

      waitForParagraph(1, "FINISHED");

      /*
      equivalent of
      import org.apache.commons.io.IOUtils
      import java.net.URL
      import java.nio.charset.Charset
      val bankText = sc.parallelize(IOUtils.toString(new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),Charset.forName("utf8")).split("\n"))
      case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

      val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(s => Bank(s(0).toInt,s(1).replaceAll("\"", ""),s(2).replaceAll("\"", ""),s(3).replaceAll("\"", ""),s(5).replaceAll("\"", "").toInt)).toDF()
      bank.registerTempTable("bank")
       */
      setTextOfParagraph(2, "import org.apache.commons.io.IOUtils\\n" +
          "import java.net.URL\\n" +
          "import java.nio.charset.Charset\\n" +
          "val bankText = sc.parallelize(IOUtils.toString(new URL(\"https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv\"),Charset.forName(\"utf8\")).split(\"\\\\n\"))\\n" +
          "case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)\\n" +
          "\\n" +
          "val bank = bankText.map(s => s.split(\";\")).filter(s => s(0) != \"\\\\\"age\\\\\"\").map(s => Bank(s(0).toInt,s(1).replaceAll(\"\\\\\"\", \"\"),s(2).replaceAll(\"\\\\\"\", \"\"),s(3).replaceAll(\"\\\\\"\", \"\"),s(5).replaceAll(\"\\\\\"\", \"\").toInt)).toDF()\\n" +
          "bank.registerTempTable(\"bank\")");
      runParagraph(2);

      try {
        waitForParagraph(2, "FINISHED");
      } catch (TimeoutException e) {
        waitForParagraph(2, "ERROR");
        collector.checkThat("2nd Paragraph from SparkParagraphIT of testSpark status:",
            "ERROR", CoreMatchers.equalTo("FINISHED")
        );
      }

      WebElement paragraph2Result = driver.findElement(By.xpath(
          getParagraphXPath(2) + "//div[contains(@id,\"_text\")]"));

      collector.checkThat("2nd Paragraph from SparkParagraphIT of testSpark result: ",
          paragraph2Result.getText().toString(), CoreMatchers.containsString(
              "import org.apache.commons.io.IOUtils"
          )
      );

    } catch (Exception e) {
      handleException("Exception in SparkParagraphIT while testSpark", e);
    }
  }

  @Ignore(value="Contains UI specific values")
  @Test
  public void testPySpark() throws Exception {
    try {
      setTextOfParagraph(1, "%pyspark\\n" +
          "for x in range(0, 3):\\n" +
          "    print(\"test loop %d\" % (x))");

      runParagraph(1);

      try {
        waitForParagraph(1, "FINISHED");
      } catch (TimeoutException e) {
        waitForParagraph(1, "ERROR");
        collector.checkThat("Paragraph from SparkParagraphIT of testPySpark status: ",
            "ERROR", CoreMatchers.equalTo("FINISHED")
        );
      }

      WebElement paragraph1Result = driver.findElement(By.xpath(
          getParagraphXPath(1) + "//div[contains(@id,\"_text\")]"));
      collector.checkThat("Paragraph from SparkParagraphIT of testPySpark result: ",
          paragraph1Result.getText().toString(), CoreMatchers.containsString("test loop 0\ntest loop 1\ntest loop 2")
      );

      // the last statement's evaluation result is printed
      setTextOfParagraph(2, "%pyspark\\n" +
          "sc.version\\n" +
          "1+1");
      runParagraph(2);
      try {
        waitForParagraph(2, "FINISHED");
      } catch (TimeoutException e) {
        waitForParagraph(2, "ERROR");
        collector.checkThat("Paragraph from SparkParagraphIT of testPySpark status: ",
                "ERROR", CoreMatchers.equalTo("FINISHED")
        );
      }
      WebElement paragraph2Result = driver.findElement(By.xpath(
              getParagraphXPath(2) + "//div[contains(@id,\"_text\")]"));
      collector.checkThat("Paragraph from SparkParagraphIT of testPySpark result: ",
          paragraph2Result.getText().toString(), CoreMatchers.equalTo("2")
      );

    } catch (Exception e) {
      handleException("Exception in SparkParagraphIT while testPySpark", e);
    }
  }

  @Ignore(value="Contains UI specific values")
  @Test
  public void testCancelPyspark() throws Exception {
    try {
      setTextOfParagraph(1, "%pyspark\\nimport time\\nfor i in range(0, 30):\\n\\ttime.sleep(1)");
      driver.findElement(By.xpath(getParagraphXPath(1) + "//span[@class='icon-settings']")).click();
      driver.findElement(By.xpath(getParagraphXPath(1) + "//ul/li/a[@ng-click=\"insertNew('below')\"]"))
              .click();
      waitForParagraph(2, "READY");
      setTextOfParagraph(2, "%pyspark\\nprint(\"Hello World!\")");


      driver.findElement(By.xpath(".//*[@id='main']//button[contains(@ng-click, 'runAllParagraphs')]")).sendKeys(Keys.ENTER);
      ZeppelinITUtils.sleep(1000, false);
      driver.findElement(By.xpath("//div[@class='modal-dialog'][contains(.,'Run all paragraphs?')]" +
              "//div[@class='modal-footer']//button[contains(.,'OK')]")).click();
      waitForParagraph(1, "RUNNING");

      ZeppelinITUtils.sleep(2000, false);
      cancelParagraph(1);
      waitForParagraph(1, "ABORT");

      collector.checkThat("First paragraph status is ",
              getParagraphStatus(1), CoreMatchers.equalTo("ABORT")
      );

      collector.checkThat("Second paragraph status is ",
              getParagraphStatus(2), CoreMatchers.either(CoreMatchers.equalTo("PENDING"))
                      .or(CoreMatchers.equalTo("READY"))
      );

      driver.navigate().refresh();
      ZeppelinITUtils.sleep(3000, false);

    } catch (Exception e) {
      handleException("Exception in SparkParagraphIT while testCancelPyspark", e);
    }
  }

  @Ignore(value="Contains UI specific values")
  @Test
  public void testSqlSpark() throws Exception {
    try {
      setTextOfParagraph(1,"%sql\\n" +
          "select * from bank limit 1");
      runParagraph(1);

      try {
        waitForParagraph(1, "FINISHED");
      } catch (TimeoutException e) {
        waitForParagraph(1, "ERROR");
        collector.checkThat("Paragraph from SparkParagraphIT of testSqlSpark status: ",
            "ERROR", CoreMatchers.equalTo("FINISHED")
        );
      }

      // Age, Job, Marital, Education, Balance
      List<WebElement> tableHeaders = driver.findElements(By.cssSelector("span.ui-grid-header-cell-label"));
      String headerNames = "";

      for(WebElement header : tableHeaders) {
        headerNames += header.getText().toString() + "|";
      }

      collector.checkThat("Paragraph from SparkParagraphIT of testSqlSpark result: ",
          headerNames, CoreMatchers.equalTo("age|job|marital|education|balance|"));
    } catch (Exception e) {
      handleException("Exception in SparkParagraphIT while testSqlSpark", e);
    }
  }

  @Ignore(value="Contains UI specific values")
  @Test
  public void testDep() throws Exception {
    try {
      // restart spark interpreter before running %dep
      clickAndWait(By.xpath("//span[@uib-tooltip='Interpreter binding']"));
      clickAndWait(By.xpath("//div[font[contains(text(), 'spark')]]/preceding-sibling::a[@uib-tooltip='Restart']"));
      clickAndWait(By.xpath("//button[contains(.,'OK')]"));

      setTextOfParagraph(1,"%dep z.load(\"org.apache.commons:commons-csv:1.1\")");
      runParagraph(1);

      try {
        waitForParagraph(1, "FINISHED");
        WebElement paragraph1Result = driver.findElement(By.xpath(getParagraphXPath(1) +
            "//div[contains(@id,'_text')]"));
        collector.checkThat("Paragraph from SparkParagraphIT of testSqlSpark result: ",
            paragraph1Result.getText(), CoreMatchers.containsString("res0: com.teragrep.zep_01.dep.Dependency = com.teragrep.zep_01.dep.Dependency"));

        setTextOfParagraph(2, "import org.apache.commons.csv.CSVFormat");
        runParagraph(2);

        try {
          waitForParagraph(2, "FINISHED");
          WebElement paragraph2Result = driver.findElement(By.xpath(getParagraphXPath(2) +
              "//div[contains(@id,'_text')]"));
          collector.checkThat("Paragraph from SparkParagraphIT of testSqlSpark result: ",
              paragraph2Result.getText(), CoreMatchers.equalTo("import org.apache.commons.csv.CSVFormat"));

        } catch (TimeoutException e) {
          waitForParagraph(2, "ERROR");
          collector.checkThat("Second paragraph from SparkParagraphIT of testDep status: ",
              "ERROR", CoreMatchers.equalTo("FINISHED")
          );
        }

      } catch (TimeoutException e) {
        waitForParagraph(1, "ERROR");
        collector.checkThat("First paragraph from SparkParagraphIT of testDep status: ",
            "ERROR", CoreMatchers.equalTo("FINISHED")
        );
      }
    } catch (Exception e) {
      handleException("Exception in SparkParagraphIT while testDep", e);
    }
  }
}
