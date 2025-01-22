package org.apache.zeppelin.interpreter.xref.remote;

import org.apache.zeppelin.display.AngularObjectRegistryListener;
import org.apache.zeppelin.interpreter.thrift.LibraryMetadata;
import org.apache.zeppelin.interpreter.thrift.ParagraphInfo;
import org.apache.zeppelin.interpreter.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEventService;
import org.apache.zeppelin.interpreter.xref.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.xref.Type;
import org.apache.zeppelin.resource.ResourcePoolConnector;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface RemoteInterpreterEventClient
        extends ResourcePoolConnector, AngularObjectRegistryListener, AutoCloseable {

    <R> R callRemoteFunction(RemoteFunction<R, RemoteInterpreterEventService.Client> func);

    void setIntpGroupId(String intpGroupId);

    void registerInterpreterProcess(RegisterInfo registerInfo);

    void unRegisterInterpreterProcess();

    void sendWebUrlInfo(String webUrl);

    List<ParagraphInfo> getParagraphList(String user, String noteId);

    List<LibraryMetadata> getAllLibraryMetadatas(String interpreter);

    ByteBuffer getLibrary(String interpreter, String libraryName);

    void onInterpreterOutputAppend(
            String noteId, String paragraphId, int outputIndex, String output
    );

    void onInterpreterOutputUpdate(
            String noteId, String paragraphId, int outputIndex, Type type, String output
    );

    void onInterpreterOutputUpdateAll(
            String noteId, String paragraphId, List<InterpreterResultMessage> messages
    );

    void runParagraphs(
            String noteId, List<String> paragraphIds, List<Integer> paragraphIndices, String curParagraphId
    );

    void checkpointOutput(String noteId, String paragraphId);

    void onParaInfosReceived(Map<String, String> infos);

    void updateParagraphConfig(String noteId, String paragraphId, Map<String, String> config);

}
