package com.teragrep.zep_01;

import com.teragrep.zep_01.notebook.Result;
import com.teragrep.zep_01.notebook.Script;
import jakarta.json.JsonObject;
import com.teragrep.zep_01.interpreter.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

public class ScriptTest {
    private final Properties properties = new Properties();
    private final InterpreterContext interpreterContext = InterpreterContext.builder().build();

    private final InterpreterSetting setting = new InterpreterSetting.Builder()
            .setName("echo")
            .setGroup("test")
            .setProperties(properties)
            .setInterpreterInfos(new ArrayList<>())
            .create();

    private final ExecutionContext context = new ExecutionContext("user","notebookId",setting.getName());
    private final EchoInterpreter interpreter = new EchoInterpreter(properties);

    private final EchoInterpreter errorInterpreter;
    private final String scriptText = "echo this text please";
    private final Script script = new Script(scriptText,interpreter,interpreterContext);

    private final Script errorScript;

    public ScriptTest() {

        errorInterpreter = new EchoInterpreter(new Properties());
        errorInterpreter.setProperty("zeppelin.interpreter.echo.fail","true");
        errorScript = new Script(scriptText,errorInterpreter,interpreterContext);
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
    public void executeTest() {
        assertNoExceptionIsThrown(()->{
            Result result = script.execute();
            Assert.assertEquals(InterpreterResult.Code.SUCCESS,result.code());
            Assert.assertEquals("%text "+scriptText,result.messages().get(0).toString());
        });
    }

    @Test
    public void executeErrorTest() {
        assertNoExceptionIsThrown(()->{
            Result result = errorScript.execute();
            Assert.assertEquals(InterpreterResult.Code.ERROR,result.code());
            Assert.assertEquals(0,result.messages().size());
        });
    }

    @Test
    public void jsonTest() {
        JsonObject json = script.json();
        Assert.assertEquals(scriptText,json.getString("text"));
        Assert.assertEquals(interpreter.getClassName(),json.getJsonObject("interpreter").getString("className"));
        Assert.assertEquals(interpreter.getProperties(),json.getJsonObject("interpreter").get("properties"));

        JsonObject errorJson = errorScript.json();
        Assert.assertEquals(scriptText,errorJson.getString("text"));
        Assert.assertEquals(errorInterpreter.getClassName(),errorJson.getJsonObject("interpreter").getString("className"));
        Assert.assertEquals("{\"zeppelin.interpreter.echo.fail\":\"true\"}",errorJson.getJsonObject("interpreter").getJsonObject("properties").toString());
    }
}