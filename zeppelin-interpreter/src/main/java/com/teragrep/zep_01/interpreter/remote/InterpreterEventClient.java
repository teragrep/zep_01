package com.teragrep.zep_01.interpreter.remote;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectRegistryListener;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import com.teragrep.zep_01.interpreter.thrift.LibraryMetadata;
import com.teragrep.zep_01.interpreter.thrift.ParagraphInfo;
import com.teragrep.zep_01.interpreter.thrift.RegisterInfo;
import com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterEventService;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceId;
import com.teragrep.zep_01.resource.ResourcePoolConnector;
import com.teragrep.zep_01.resource.ResourceSet;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface InterpreterEventClient extends ResourcePoolConnector, AngularObjectRegistryListener, AutoCloseable {
    <R> R callRemoteFunction(PooledRemoteClient.RemoteFunction<R, RemoteInterpreterEventService.Client> func);

    void setIntpGroupId(final String intpGroupId);

    void registerInterpreterProcess(final RegisterInfo registerInfo);

    void unRegisterInterpreterProcess();

    void sendWebUrlInfo(final String webUrl);

    @Override
    ResourceSet getAllResources();

    List<ParagraphInfo> getParagraphList(final String user, final String noteId);

    List<LibraryMetadata> getAllLibraryMetadatas(final String interpreter);

    ByteBuffer getLibrary(final String interpreter, final String libraryName);

    @Override
    Object readResource(final ResourceId resourceId);

    @Override
    Object invokeMethod(
            final ResourceId resourceId,
            final String methodName,
            final Class[] paramTypes,
            final Object[] params);

    @Override
    Resource invokeMethod(
            final ResourceId resourceId,
            final String methodName,
            final Class[] paramTypes,
            final Object[] params,
            final String returnResourceName);

    void onInterpreterOutputAppend(
            final String noteId, final String paragraphId, final int outputIndex, final String output);

    void onInterpreterOutputUpdate(
            final String noteId, final String paragraphId, final int outputIndex,
            final InterpreterResult.Type type, final String output);

    void onInterpreterOutputUpdateAll(
            final String noteId, final String paragraphId, final List<InterpreterResultMessage> messages);

    void runParagraphs(final String noteId,
                       final List<String> paragraphIds,
                       final List<Integer> paragraphIndices,
                       final String curParagraphId);

    void checkpointOutput(final String noteId, final String paragraphId);

    void onParaInfosReceived(final Map<String, String> infos);

    @Override
    void onAddAngularObject(final String interpreterGroupId, final AngularObject angularObject);

    @Override
    void onUpdateAngularObject(final String interpreterGroupId, final AngularObject angularObject);

    @Override
    void onRemoveAngularObject(final String interpreterGroupId, final AngularObject angularObject);

    void updateParagraphConfig(final String noteId, final String paragraphId, final Map<String, String> config);

    @Override
    void close();
}
