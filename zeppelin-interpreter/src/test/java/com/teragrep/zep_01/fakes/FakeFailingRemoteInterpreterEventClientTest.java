package com.teragrep.zep_01.fakes;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import com.teragrep.zep_01.interpreter.remote.PooledRemoteClient;
import com.teragrep.zep_01.interpreter.thrift.*;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceId;
import com.teragrep.zep_01.resource.ResourceSet;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class FakeFailingRemoteInterpreterEventClientTest {

    @Test
    public void testUnregisterInterpreter(){
        final RuntimeException exception = new RuntimeException();
        final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
        Assertions.assertThrows(RuntimeException.class, client::unRegisterInterpreterProcess);
    }

    @Test
    public void testException(){
        final RuntimeException exception = new RuntimeException();
        final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
        Assertions.assertEquals(exception,client.exception());
    }

   @Test
    public void testCallRemoteFunction() {
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertThrows(exception.getClass(),() -> client.callRemoteFunction(new PooledRemoteClient.RemoteFunction<Object, RemoteInterpreterEventService.Client>() {
           @Override
           public Object call(final RemoteInterpreterEventService.Client client) throws InterpreterRPCException, TException {
               return new Object();
           }
       }));
   }
   @Test
    public void testGetAllResources() {
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertEquals(new ResourceSet(),client.getAllResources());
    }

   @Test
    public void getParagraphList() {
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertEquals(new ArrayList<>(),client.getParagraphList("test","test"));
    }

   @Test
    public void getAllLibraryMetadatas() {
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertEquals(new ArrayList<>(),client.getAllLibraryMetadatas("test"));
    }

   @Test
    public void getLibrary() {
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertEquals(ByteBuffer.allocate(0),client.getLibrary("test", "test"));
    }

   @Test
    public void readResource() {
        final JsonObject resourceIdJson = Json.createObjectBuilder().add("resourcePoolId","test").add("name","test").add("noteId","teste").add("paragraphId","test").build();
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       final Object result = client.readResource(ResourceId.fromJson(resourceIdJson.toString()));
       Assertions.assertNotNull(result);
       Assertions.assertEquals(Object.class,result.getClass());
    }

   @Test
    public void testInvokeMethod() {
       final JsonObject resourceIdJson = Json.createObjectBuilder().add("resourcePoolId","test").add("name","test").add("noteId","teste").add("paragraphId","test").build();
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       final Class[] paramTypes = new Class[]{};
       final Object[] params = new Object[][]{};
       final Object result = client.invokeMethod(ResourceId.fromJson(resourceIdJson.toString()),"methodName",paramTypes,params);
       Assertions.assertNotNull(result);
       Assertions.assertEquals(Object.class,result.getClass());
    }

   @Test
    public void testInvokeResourceMethod() {
       final JsonObject resourceIdJson = Json.createObjectBuilder().add("resourcePoolId","test").add("name","test").add("noteId","teste").add("paragraphId","test").build();
       final RuntimeException exception = new RuntimeException();
       final FakeFailingRemoteInterpreterEventClient client = new FakeFailingRemoteInterpreterEventClient(exception);
       Assertions.assertThrows(exception.getClass(),()->{client.invokeMethod(ResourceId.fromJson(resourceIdJson.toString()),"methodName",null,null,"test");});
   }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(FakeFailingRemoteInterpreterEventClient.class).verify();
    }
}