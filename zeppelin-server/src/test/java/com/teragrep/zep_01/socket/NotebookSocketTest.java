package com.teragrep.zep_01.socket;

import com.teragrep.zep_01.socket.fakes.FakeHttpServletRequest;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class NotebookSocketTest {
    // calling send() before a call to onWebSocketConnect() is received should result in an IOException
    @Test
    public void testCallingSendWithUninitializedConnectionThrowsIOException(){
        NotebookSocket socket = new NotebookSocket(new FakeHttpServletRequest(),new FakeNotebookSocketListener());
        Throwable error = Assertions.assertThrows(IOException.class,()->{socket.send("LETMEIN");});
        Assertions.assertEquals("Tried to send a message to a Websocket connection with no established Session!",error.getMessage());
    }
}