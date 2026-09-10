package com.teragrep.zep_01.fakes;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import com.teragrep.zep_01.interpreter.remote.PooledRemoteClient;
import com.teragrep.zep_01.interpreter.remote.InterpreterEventClient;
import com.teragrep.zep_01.interpreter.thrift.LibraryMetadata;
import com.teragrep.zep_01.interpreter.thrift.ParagraphInfo;
import com.teragrep.zep_01.interpreter.thrift.RegisterInfo;
import com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterEventService;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceId;
import com.teragrep.zep_01.resource.ResourceSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class FakeFailingRemoteInterpreterEventClient implements InterpreterEventClient {
    private final RuntimeException exception;
    public FakeFailingRemoteInterpreterEventClient(final RuntimeException exception){
        this.exception = exception;
    }

    @Override
    public void unRegisterInterpreterProcess() {
        throw exception;
    }

    public RuntimeException exception(){
        return exception;
    }

    @Override
    public <R> R callRemoteFunction(final PooledRemoteClient.RemoteFunction<R, RemoteInterpreterEventService.Client> func) {
        throw new RuntimeException("Not implemented by fake");
    }

    @Override
    public void setIntpGroupId(final String intpGroupId) {
    }

    @Override
    public void registerInterpreterProcess(final RegisterInfo registerInfo) {
    }

    @Override
    public void sendWebUrlInfo(final String webUrl) {
    }

    @Override
    public ResourceSet getAllResources() {
        return new ResourceSet();
    }

    @Override
    public List<ParagraphInfo> getParagraphList(final String user, final String noteId) {
        return new ArrayList<>();
    }

    @Override
    public List<LibraryMetadata> getAllLibraryMetadatas(final String interpreter) {
        return new ArrayList<>();
    }

    @Override
    public ByteBuffer getLibrary(final String interpreter, final String libraryName) {
        return ByteBuffer.allocate(0);
    }

    @Override
    public Object readResource(final ResourceId resourceId) {
        return new Object();
    }

    @Override
    public Object invokeMethod(final ResourceId resourceId, final String methodName, final Class[] paramTypes, final Object[] params) {
        return new Object();
    }

    @Override
    public Resource invokeMethod(final ResourceId resourceId, final String methodName, final Class[] paramTypes, final Object[] params, final String returnResourceName) {
        throw new RuntimeException("Not implemented by fake");
    }

    @Override
    public void onInterpreterOutputAppend(final String noteId, final String paragraphId, final int outputIndex, final String output) {
    }

    @Override
    public void onInterpreterOutputUpdate(final String noteId, final String paragraphId, final int outputIndex, final InterpreterResult.Type type, final String output) {
    }

    @Override
    public void onInterpreterOutputUpdateAll(final String noteId, final String paragraphId, final List<InterpreterResultMessage> messages) {
    }

    @Override
    public void runParagraphs(final String noteId, final List<String> paragraphIds, final List<Integer> paragraphIndices, final String curParagraphId) {
    }

    @Override
    public void checkpointOutput(final String noteId, final String paragraphId) {
    }

    @Override
    public void onParaInfosReceived(final Map<String, String> infos) {
    }

    @Override
    public void onAddAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
    }

    @Override
    public void onUpdateAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
    }

    @Override
    public void onRemoveAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
    }

    @Override
    public void updateParagraphConfig(final String noteId, final String paragraphId, final Map<String, String> config) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FakeFailingRemoteInterpreterEventClient that = (FakeFailingRemoteInterpreterEventClient) o;
        return Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(exception);
    }
}