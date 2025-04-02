package com.teragrep.zep_01;

import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.notebook.Result;
import com.teragrep.zep_01.notebook.Script;
import jakarta.json.JsonObject;
import com.teragrep.zep_01.interpreter.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ParagraphTest {
    private final String paragraphId = "paragraph_10231239";
    private final String paragraphName = "testParagraph";
    private final Result emptyResult = new Result(InterpreterResult.Code.SUCCESS,new ArrayList<>());
    private final String scriptText = "echo this text please";
    private final Properties properties = new Properties();
    private final InterpreterContext interpreterContext = InterpreterContext.builder().build();

    private final InterpreterSetting setting = new InterpreterSetting.Builder()
            .setName("echo")
            .setGroup("test")
            .setProperties(properties)
            .setInterpreterInfos(new ArrayList<>())
            .create();

    private final ExecutionContext context = new ExecutionContext("user","notebookId",setting.getName());
    private final ManagedInterpreterGroup interpreterGroup = setting.getOrCreateInterpreterGroup(context);
    private final EchoInterpreter interpreter = new EchoInterpreter(properties);
    private final Script testScript1 = new Script(scriptText,interpreter,interpreterContext);
    private final Paragraph paragraph = new Paragraph(paragraphId,paragraphName,emptyResult,testScript1);

    @Before
    public void setup(){
        interpreter.setInterpreterGroup(interpreterGroup);
    }

    // This method is a custom JUnit4 implementation of Assertions.assertDoesNotThrow, since JUnit4 does not have a similar built-in method.
    private void assertNoExceptionIsThrown(NotebookRepositoryTest.Executable executable) {
        try {
            executable.execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getClass().getSimpleName() + " was thrown");
        }
    }
    @Test
    public void runTest() {
        assertNoExceptionIsThrown(()->{
            Result result = paragraph.run();
            assertEquals(InterpreterResult.Code.SUCCESS,result.code());
            assertEquals("%text "+scriptText,result.messages().get(0).toString());
        });
    }

    @Test
    public void json() {
        JsonObject json = paragraph.json();
        System.out.println(json.toString());
        assertEquals(paragraphId,json.getString("id"));
        assertEquals(paragraphName,json.getString("title"));
        assertEquals(paragraph.script().json(),json.getJsonObject("script"));
        assertEquals(paragraph.result().json(),json.getJsonObject("results"));
    }
}