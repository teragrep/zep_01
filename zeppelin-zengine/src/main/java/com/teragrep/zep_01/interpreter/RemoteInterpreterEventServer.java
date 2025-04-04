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

package com.teragrep.zep_01.interpreter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.remote.AppendOutputRunner;
import com.teragrep.zep_01.interpreter.remote.InvokeResourceMethodEventMessage;
import com.teragrep.zep_01.interpreter.remote.RemoteAngularObject;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterProcess;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterProcessListener;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterUtils;
import com.teragrep.zep_01.interpreter.thrift.InterpreterRPCException;
import com.teragrep.zep_01.interpreter.thrift.LibraryMetadata;
import com.teragrep.zep_01.interpreter.thrift.ParagraphInfo;
import com.teragrep.zep_01.interpreter.thrift.RegisterInfo;
import com.teragrep.zep_01.interpreter.thrift.OutputAppendEvent;
import com.teragrep.zep_01.interpreter.thrift.OutputUpdateAllEvent;
import com.teragrep.zep_01.interpreter.thrift.OutputUpdateEvent;
import com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterEventService;
import com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterResultMessage;
import com.teragrep.zep_01.interpreter.thrift.RunParagraphsEvent;
import com.teragrep.zep_01.interpreter.thrift.WebUrlInfo;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.resource.RemoteResource;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceId;
import com.teragrep.zep_01.resource.ResourcePool;
import com.teragrep.zep_01.resource.ResourceSet;
import com.teragrep.zep_01.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RemoteInterpreterEventServer implements RemoteInterpreterEventService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteInterpreterEventServer.class);
  private static final Gson GSON = new Gson();

  private String portRange;
  private int port;
  private String host;
  private ZeppelinConfiguration zConf;
  private TThreadPoolServer thriftServer;
  private InterpreterSettingManager interpreterSettingManager;

  private final ScheduledExecutorService appendService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> appendFuture;
  private AppendOutputRunner runner;
  private final RemoteInterpreterProcessListener listener;


  public RemoteInterpreterEventServer(ZeppelinConfiguration zConf,
                                      InterpreterSettingManager interpreterSettingManager) {
    this.zConf = zConf;
    this.portRange = zConf.getZeppelinServerRPCPortRange();
    this.interpreterSettingManager = interpreterSettingManager;
    this.listener = interpreterSettingManager.getRemoteInterpreterProcessListener();
  }

  public void start() throws IOException {
    Thread startingThread = new Thread() {
      @Override
      public void run() {
        try (TServerSocket tSocket = new TServerSocket(RemoteInterpreterUtils.findAvailablePort(portRange))){
          port = tSocket.getServerSocket().getLocalPort();
          host = RemoteInterpreterUtils.findAvailableHostAddress();
          LOGGER.info("InterpreterEventServer is starting at {}:{}", host, port);
          RemoteInterpreterEventService.Processor<RemoteInterpreterEventServer> processor =
              new RemoteInterpreterEventService.Processor<>(RemoteInterpreterEventServer.this);
          thriftServer = new TThreadPoolServer(
              new TThreadPoolServer.Args(tSocket).processor(processor));
          thriftServer.serve();
        } catch (IOException | TTransportException e ) {
          throw new RuntimeException("Fail to create TServerSocket", e);
        }
        LOGGER.info("ThriftServer-Thread finished");
      }
    };
    startingThread.start();
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < 30 * 1000) {
      if (thriftServer != null && thriftServer.isServing()) {
        break;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    if (thriftServer != null && !thriftServer.isServing()) {
      throw new IOException("Fail to start InterpreterEventServer in 30 seconds.");
    }
    LOGGER.info("RemoteInterpreterEventServer is started");

    runner = new AppendOutputRunner(listener);
    appendFuture = appendService.scheduleWithFixedDelay(
        runner, 0, AppendOutputRunner.BUFFER_TIME_MS, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
    if (appendFuture != null) {
      appendFuture.cancel(true);
    }
    appendService.shutdownNow();
    LOGGER.info("RemoteInterpreterEventServer is stopped");
  }


  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  @Override
  public void registerInterpreterProcess(RegisterInfo registerInfo) throws InterpreterRPCException, TException {
    InterpreterGroup interpreterGroup =
        interpreterSettingManager.getInterpreterGroupById(registerInfo.getInterpreterGroupId());
    if (interpreterGroup == null) {
      LOGGER.warn("Unable to register interpreter process, because no such interpreterGroup: {}",
              registerInfo.getInterpreterGroupId());
      return;
    }
    RemoteInterpreterProcess interpreterProcess =
        ((ManagedInterpreterGroup) interpreterGroup).getInterpreterProcess();
    if (interpreterProcess == null) {
      LOGGER.warn("Unable to register interpreter process, because no interpreter process associated with " +
              "interpreterGroup: {}", registerInfo.getInterpreterGroupId());
      return;
    }
    LOGGER.info("Register interpreter process: {}:{}, interpreterGroup: {}",
            registerInfo.getHost(), registerInfo.getPort(), registerInfo.getInterpreterGroupId());
    interpreterProcess.processStarted(registerInfo.port, registerInfo.host);
  }

  @Override
  public void unRegisterInterpreterProcess(String intpGroupId) throws InterpreterRPCException, TException {
    LOGGER.info("Unregister interpreter process: {}", intpGroupId);
    InterpreterGroup interpreterGroup =
            interpreterSettingManager.getInterpreterGroupById(intpGroupId);
    if (interpreterGroup == null) {
      LOGGER.warn("Unable to unregister interpreter process because no such interpreterGroup: {}",
              intpGroupId);
      return;
    }
    // Close RemoteInterpreter when RemoteInterpreterServer already timeout.
    // Otherwise the ProgressBar will be missing when rerun after the RemoteInterpreterServer timeout
    // and old RemoteInterpreterGroups will always alive after GC.
    interpreterGroup.close();
    interpreterSettingManager.removeInterpreterGroup(intpGroupId);
  }

  @Override
  public void sendWebUrl(WebUrlInfo weburlInfo) throws InterpreterRPCException, TException {
    InterpreterGroup interpreterGroup =
            interpreterSettingManager.getInterpreterGroupById(weburlInfo.getInterpreterGroupId());
    if (interpreterGroup == null) {
      LOGGER.warn("Unable to sendWebUrl, because no such interpreterGroup: {}",
              weburlInfo.getInterpreterGroupId());
      return;
    }
    interpreterGroup.setWebUrl(weburlInfo.getWeburl());
  }

  @Override
  public void appendOutput(OutputAppendEvent event) throws InterpreterRPCException, TException {
    if (event.getAppId() == null) {
      runner.appendBuffer(
          event.getNoteId(), event.getParagraphId(), event.getIndex(), event.getData());
    }
  }

  @Override
  public void updateOutput(OutputUpdateEvent event) throws InterpreterRPCException, TException {
    if (event.getAppId() == null) {
      listener.onOutputUpdated(event.getNoteId(), event.getParagraphId(), event.getIndex(),
          InterpreterResult.Type.valueOf(event.getType()), event.getData());
    }
  }

  @Override
  public void updateAllOutput(OutputUpdateAllEvent event) throws InterpreterRPCException, TException {
    listener.onOutputClear(event.getNoteId(), event.getParagraphId());
    for (int i = 0; i < event.getMsg().size(); i++) {
      RemoteInterpreterResultMessage msg = event.getMsg().get(i);
      listener.onOutputUpdated(event.getNoteId(), event.getParagraphId(), i,
          InterpreterResult.Type.valueOf(msg.getType()), msg.getData());
    }
  }

  @Override
  public void checkpointOutput(String noteId, String paragraphId) throws InterpreterRPCException, TException {
    listener.checkpointOutput(noteId, paragraphId);
  }

  @Override
  public void runParagraphs(RunParagraphsEvent event) throws InterpreterRPCException, TException {
    try {
      listener.runParagraphs(event.getNoteId(), event.getParagraphIndices(),
          event.getParagraphIds(), event.getCurParagraphId());
      if (InterpreterContext.get() != null) {
        LOGGER.info("complete runParagraphs.{} {}", InterpreterContext.get().getParagraphId(), event);
      } else {
        LOGGER.info("complete runParagraphs.{}", event);
      }
    } catch (IOException e) {
      throw new InterpreterRPCException(e.toString());
    }
  }

  @Override
  public void addAngularObject(String intpGroupId, String json) throws InterpreterRPCException, TException {
    LOGGER.debug("Add AngularObject, interpreterGroupId: {}, json: {}", intpGroupId, json);
    AngularObject<?> angularObject = AngularObject.fromJson(json);
    InterpreterGroup interpreterGroup =
        interpreterSettingManager.getInterpreterGroupById(intpGroupId);
    if (interpreterGroup == null) {
      LOGGER.warn("Invalid InterpreterGroupId: {}", intpGroupId);
      return;
    }
    interpreterGroup.getAngularObjectRegistry().add(angularObject.getName(),
        angularObject.get(), angularObject.getNoteId(), angularObject.getParagraphId());

    if (angularObject.getNoteId() != null) {
      try {
        Note note = interpreterSettingManager.getNotebook().getNote(angularObject.getNoteId());
        if (note != null) {
          note.addOrUpdateAngularObject(intpGroupId, angularObject);
          interpreterSettingManager.getNotebook().saveNote(note, AuthenticationInfo.ANONYMOUS);
        }
      } catch (IOException e) {
        LOGGER.error("Fail to get note: {}", angularObject.getNoteId());
      }
    }
  }

  @Override
  public void updateAngularObject(String intpGroupId, String json) throws InterpreterRPCException, TException {
    AngularObject<?> angularObject = AngularObject.fromJson(json);
    InterpreterGroup interpreterGroup =
        interpreterSettingManager.getInterpreterGroupById(intpGroupId);
    if (interpreterGroup == null) {
      throw new InterpreterRPCException("Invalid InterpreterGroupId: " + intpGroupId);
    }
    AngularObject localAngularObject = interpreterGroup.getAngularObjectRegistry().get(
        angularObject.getName(), angularObject.getNoteId(), angularObject.getParagraphId());
    if (localAngularObject instanceof RemoteAngularObject) {
      // to avoid ping-pong loop
      ((RemoteAngularObject) localAngularObject).set(
          angularObject.get(), true, false);
    } else {
      localAngularObject.set(angularObject.get());
    }

    if (angularObject.getNoteId() != null) {
      try {
        Note note = interpreterSettingManager.getNotebook().getNote(angularObject.getNoteId());
        if (note != null) {
          note.addOrUpdateAngularObject(intpGroupId, angularObject);
          interpreterSettingManager.getNotebook().saveNote(note, AuthenticationInfo.ANONYMOUS);
        }
      } catch (IOException e) {
        LOGGER.error("Fail to get note: {}", angularObject.getNoteId());
      }
    }
  }

  @Override
  public void removeAngularObject(String intpGroupId,
                                  String noteId,
                                  String paragraphId,
                                  String name) throws InterpreterRPCException, TException {
    InterpreterGroup interpreterGroup =
        interpreterSettingManager.getInterpreterGroupById(intpGroupId);
    if (interpreterGroup == null) {
      throw new InterpreterRPCException("Invalid InterpreterGroupId: " + intpGroupId);
    }
    interpreterGroup.getAngularObjectRegistry().remove(name, noteId, paragraphId);

    if (noteId != null) {
      try {
        Note note = interpreterSettingManager.getNotebook().getNote(noteId);
        note.deleteAngularObject(intpGroupId, noteId, paragraphId, name);
      } catch (IOException e) {
        LOGGER.warn("Fail to get note: {}", noteId, e);
      }
    }
  }

  @Override
  public void sendParagraphInfo(String intpGroupId, String json) throws InterpreterRPCException, TException {
    InterpreterGroup interpreterGroup =
        interpreterSettingManager.getInterpreterGroupById(intpGroupId);
    if (interpreterGroup == null) {
      throw new InterpreterRPCException("Invalid InterpreterGroupId: " + intpGroupId);
    }

    Map<String, String> paraInfos = GSON.fromJson(json,
        new TypeToken<Map<String, String>>() {
        }.getType());
    String noteId = paraInfos.get("noteId");
    String paraId = paraInfos.get("paraId");
    String settingId = ((ManagedInterpreterGroup) interpreterGroup).getInterpreterSetting().getId();
    if (noteId != null && paraId != null && settingId != null) {
      listener.onParaInfosReceived(noteId, paraId, settingId, paraInfos);
    }
  }

  @Override
  public List<String> getAllResources(String intpGroupId) throws InterpreterRPCException, TException {
    ResourceSet resourceSet = getAllResourcePoolExcept(intpGroupId);
    List<String> resourceList = new LinkedList<>();
    for (Resource r : resourceSet) {
      resourceList.add(r.toJson());
    }
    return resourceList;
  }

  @Override
  public ByteBuffer getResource(String resourceIdJson) throws InterpreterRPCException, TException {
    ResourceId resourceId = ResourceId.fromJson(resourceIdJson);
    Object o = getResource(resourceId);
    ByteBuffer obj;
    if (o == null) {
      obj = ByteBuffer.allocate(0);
    } else {
      try {
        obj = Resource.serializeObject(o);
      } catch (IOException e) {
        throw new InterpreterRPCException(e.toString());
      }
    }
    return obj;
  }

  /**
   *
   * @param intpGroupId caller interpreter group id
   * @param invokeMethodJson invoke information
   * @return
   * @throws TException
   */
  @Override
  public ByteBuffer invokeMethod(String intpGroupId, String invokeMethodJson)
          throws InterpreterRPCException, TException {
    InvokeResourceMethodEventMessage invokeMethodMessage =
        InvokeResourceMethodEventMessage.fromJson(invokeMethodJson);
    Object ret = invokeResourceMethod(intpGroupId, invokeMethodMessage);
    ByteBuffer obj = null;
    if (ret == null) {
      obj = ByteBuffer.allocate(0);
    } else {
      try {
        obj = Resource.serializeObject(ret);
      } catch (IOException e) {
        LOGGER.error("invokeMethod failed", e);
      }
    }
    return obj;
  }

  @Override
  public List<ParagraphInfo> getParagraphList(String user, String noteId)
          throws InterpreterRPCException, TException {
    LOGGER.info("get paragraph list from remote interpreter noteId: {}, user = {}",noteId, user);

    if (user != null && noteId != null) {
      List<ParagraphInfo> paragraphInfos = null;
      try {
        paragraphInfos = listener.getParagraphList(user, noteId);
      } catch (IOException e) {
       throw new InterpreterRPCException(e.toString());
      }
      return paragraphInfos;
    } else {
      LOGGER.error("user or noteId is null!");
      return Collections.emptyList();
    }
  }

  private Object invokeResourceMethod(String intpGroupId,
                                      final InvokeResourceMethodEventMessage message) {
    final ResourceId resourceId = message.resourceId;
    ManagedInterpreterGroup intpGroup =
        interpreterSettingManager.getInterpreterGroupById(resourceId.getResourcePoolId());
    if (intpGroup == null) {
      return null;
    }

    RemoteInterpreterProcess remoteInterpreterProcess = intpGroup.getRemoteInterpreterProcess();
    if (remoteInterpreterProcess == null) {
      ResourcePool localPool = intpGroup.getResourcePool();
      if (localPool != null) {
        Resource res = localPool.get(resourceId.getName());
        if (res != null) {
          try {
            return res.invokeMethod(
                message.methodName,
                message.getParamTypes(),
                message.params,
                message.returnResourceName);
          } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
          }
        } else {
          // object is null. can't invoke any method
          LOGGER.error("Can't invoke method {} on null object", message.methodName);
          return null;
        }
      } else {
        LOGGER.error("no resource pool");
        return null;
      }
    } else if (remoteInterpreterProcess.isRunning()) {
      ByteBuffer res = remoteInterpreterProcess.callRemoteFunction(client ->
              client.resourceInvokeMethod(
                  resourceId.getNoteId(),
                  resourceId.getParagraphId(),
                  resourceId.getName(),
                  message.toJson()));

      try {
        return Resource.deserializeObject(res);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
      return null;
    }
    return null;
  }

  private Object getResource(final ResourceId resourceId) {
    ManagedInterpreterGroup intpGroup = interpreterSettingManager
        .getInterpreterGroupById(resourceId.getResourcePoolId());
    if (intpGroup == null) {
      return null;
    }
    RemoteInterpreterProcess remoteInterpreterProcess = intpGroup.getRemoteInterpreterProcess();
    ByteBuffer buffer = remoteInterpreterProcess.callRemoteFunction(client ->
            client.resourceGet(
                resourceId.getNoteId(),
                resourceId.getParagraphId(),
                resourceId.getName()));

    try {
      return Resource.deserializeObject(buffer);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return null;
  }

  private ResourceSet getAllResourcePoolExcept(String interpreterGroupId) {
    ResourceSet resourceSet = new ResourceSet();
    for (ManagedInterpreterGroup intpGroup : interpreterSettingManager.getAllInterpreterGroup()) {
      if (intpGroup.getId().equals(interpreterGroupId)) {
        continue;
      }

      RemoteInterpreterProcess remoteInterpreterProcess = intpGroup.getRemoteInterpreterProcess();
      if (remoteInterpreterProcess == null) {
        ResourcePool localPool = intpGroup.getResourcePool();
        if (localPool != null) {
          resourceSet.addAll(localPool.getAll());
        }
      } else if (remoteInterpreterProcess.isRunning()) {
        List<String> resourceList = remoteInterpreterProcess.callRemoteFunction(
                client -> client.resourcePoolGetAll());
        for (String res : resourceList) {
          resourceSet.add(RemoteResource.fromJson(res));
        }
      }
    }
    return resourceSet;
  }

  @Override
  public void updateParagraphConfig(String noteId,
                                    String paragraphId,
                                    Map<String, String> config)
          throws InterpreterRPCException, TException {
    try {
      Note note = interpreterSettingManager.getNotebook().getNote(noteId);
      note.getParagraph(paragraphId).updateConfig(config);
      interpreterSettingManager.getNotebook().saveNote(note, AuthenticationInfo.ANONYMOUS);
    } catch (Exception e) {
      LOGGER.error("Fail to updateParagraphConfig", e);
    }
  }

  @Override
  public List<LibraryMetadata> getAllLibraryMetadatas(String interpreter) throws TException {
    // FIXME: Look what this function is actually about and restore data if necessary
    return Collections.emptyList();
  }


  @Override
  public ByteBuffer getLibrary(String interpreter, String libraryName) throws TException {
    // FIXME: Look what this function is actually about and restore data if necessary
    return null;
  }

}
