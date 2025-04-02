package com.teragrep.zep_01;

import com.teragrep.zep_01.notebook.Result;
import jakarta.json.JsonObject;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class ResultTest {

    InterpreterResultMessage textMessage = new InterpreterResultMessage(InterpreterResult.Type.TEXT,"hello i am some text");
    InterpreterResultMessage htmlMessage = new InterpreterResultMessage(InterpreterResult.Type.HTML,"<div><h1>Title</h1><p>contents\ncontents\n</p></div>");
    InterpreterResultMessage tableMessage = new InterpreterResultMessage(InterpreterResult.Type.TABLE,"table_data");

    ArrayList<InterpreterResultMessage> messages = new ArrayList<>();
    Result result;
    @Before
    public void setUp(){
        messages.add(textMessage);
        messages.add(htmlMessage);
        messages.add(tableMessage);
        result = new Result(InterpreterResult.Code.SUCCESS,messages);
    }
    @Test
    public void jsonTest(){
        JsonObject json = result.json();
        Assert.assertEquals("\"SUCCESS\"", json.get("code").toString());
        Assert.assertEquals("[{\"type\":\"TEXT\",\"data\":\"hello i am some text\"},{\"type\":\"HTML\",\"data\":\"<div><h1>Title</h1><p>contents\\ncontents\\n</p></div>\"},{\"type\":\"TABLE\",\"data\":\"table_data\"}]",json.get("msg").toString());
        Assert.assertEquals(3,result.messages().size());
        Assert.assertEquals(InterpreterResult.Code.SUCCESS,result.code());
    }
}