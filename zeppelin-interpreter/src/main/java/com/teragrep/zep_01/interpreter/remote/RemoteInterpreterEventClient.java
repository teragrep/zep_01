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
package com.teragrep.zep_01.interpreter.remote;

import com.google.gson.Gson;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import com.teragrep.zep_01.interpreter.thrift.LibraryMetadata;
import com.teragrep.zep_01.interpreter.thrift.OutputAppendEvent;
import com.teragrep.zep_01.interpreter.thrift.OutputUpdateAllEvent;
import com.teragrep.zep_01.interpreter.thrift.OutputUpdateEvent;
import com.teragrep.zep_01.interpreter.thrift.ParagraphInfo;
import com.teragrep.zep_01.interpreter.thrift.RegisterInfo;
import com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterEventService;
import com.teragrep.zep_01.interpreter.thrift.RunParagraphsEvent;
import com.teragrep.zep_01.interpreter.thrift.WebUrlInfo;
import com.teragrep.zep_01.resource.RemoteResource;
import com.teragrep.zep_01.resource.Resource;
import com.teragrep.zep_01.resource.ResourceId;
import com.teragrep.zep_01.resource.ResourceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to communicate with ZeppelinServer via thrift.
 * All the methods are synchronized because thrift client is not thread safe.
 */
public class RemoteInterpreterEventClient implements
        InterpreterEventClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteInterpreterEventClient.class);
  private static final Gson GSON = new Gson();

  private PooledRemoteClient<RemoteInterpreterEventService.Client> remoteClient;
  private String intpGroupId;

  public RemoteInterpreterEventClient(String intpEventHost, int intpEventPort, int connectionPoolSize) {
    this.remoteClient = new PooledRemoteClient<>(() -> {
      TSocket transport = new TSocket(intpEventHost, intpEventPort);
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IOException(e);
      }
      TProtocol protocol = new TBinaryProtocol(transport);
      return new RemoteInterpreterEventService.Client(protocol);
    }, connectionPoolSize);
  }

  @Override
  public <R> R callRemoteFunction(PooledRemoteClient.RemoteFunction<R, RemoteInterpreterEventService.Client> func) {
    return remoteClient.callRemoteFunction(func);
  }

  @Override
  public void setIntpGroupId(String intpGroupId) {
    this.intpGroupId = intpGroupId;
  }

  @Override
  public void registerInterpreterProcess(RegisterInfo registerInfo) {
    callRemoteFunction(client -> {
      client.registerInterpreterProcess(registerInfo);
      return null;
    });
  }

  @Override
  public void unRegisterInterpreterProcess() {
    callRemoteFunction(client -> {
      client.unRegisterInterpreterProcess(intpGroupId);
      return null;
    });
  }

  @Override
  public void sendWebUrlInfo(String webUrl) {
    callRemoteFunction(client -> {
      client.sendWebUrl(new WebUrlInfo(intpGroupId, webUrl));
      return null;
    });
  }

  /**
   * Get all resources except for specific resourcePool
   *
   * @return
   */
  @Override
  public ResourceSet getAllResources() {
    try {
      List<String> resources = callRemoteFunction(client -> client.getAllResources(intpGroupId));
      ResourceSet resourceSet = new ResourceSet();
      for (String res : resources) {
        RemoteResource resource = RemoteResource.fromJson(res);
        resource.setResourcePoolConnector(this);
        resourceSet.add(resource);
      }
      return resourceSet;
    } catch (Exception e) {
      LOGGER.warn("Fail to getAllResources", e);
      return null;
    }
  }

  @Override
  public List<ParagraphInfo> getParagraphList(String user, String noteId) {
    return callRemoteFunction(client -> client.getParagraphList(user, noteId));
  }

  @Override
  public List<LibraryMetadata> getAllLibraryMetadatas(String interpreter) {
    return callRemoteFunction(client -> client.getAllLibraryMetadatas(interpreter));
  }

  @Override
  public ByteBuffer getLibrary(String interpreter, String libraryName) {
    return callRemoteFunction(client -> client.getLibrary(interpreter, libraryName));
  }

  @Override
  public Object readResource(ResourceId resourceId) {
    try {
      ByteBuffer buffer = callRemoteFunction(client -> client.getResource(resourceId.toJson()));
      return Resource.deserializeObject(buffer);
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.warn("Fail to readResource: {}", resourceId, e);
      return null;
    }
  }

  /**
   * Invoke method and save result in resourcePool as another resource
   *
   * @param resourceId
   * @param methodName
   * @param paramTypes
   * @param params
   * @return
   */
  @Override
  public Object invokeMethod(
          final ResourceId resourceId,
          final String methodName,
          final Class[] paramTypes,
          final Object[] params) {
    LOGGER.debug("Request Invoke method {} of Resource {}", methodName, resourceId.getName());

    InvokeResourceMethodEventMessage invokeMethod = new InvokeResourceMethodEventMessage(
            resourceId,
            methodName,
            paramTypes,
            params,
            null);
    try {
      ByteBuffer buffer = callRemoteFunction(client -> client.invokeMethod(intpGroupId, invokeMethod.toJson()));
      return Resource.deserializeObject(buffer);
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error("Failed to invoke method", e);
      return null;
    }
  }

  /**
   * Invoke method and save result in resourcePool as another resource
   *
   * @param resourceId
   * @param methodName
   * @param paramTypes
   * @param params
   * @param returnResourceName
   * @return
   */
  @Override
  public Resource invokeMethod(
          final ResourceId resourceId,
          final String methodName,
          final Class[] paramTypes,
          final Object[] params,
          final String returnResourceName) {
    LOGGER.debug("Request Invoke method {} of Resource {}", methodName, resourceId.getName());

    InvokeResourceMethodEventMessage invokeMethod = new InvokeResourceMethodEventMessage(
            resourceId,
            methodName,
            paramTypes,
            params,
            returnResourceName);

    try {
      ByteBuffer serializedResource = callRemoteFunction(client -> client.invokeMethod(intpGroupId, invokeMethod.toJson()));
      Resource deserializedResource = (Resource) Resource.deserializeObject(serializedResource);
      RemoteResource remoteResource = RemoteResource.fromJson(GSON.toJson(deserializedResource));
      remoteResource.setResourcePoolConnector(this);

      return remoteResource;
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error("Failed to invoke method", e);
      return null;
    }
  }

  @Override
  public void onInterpreterOutputAppend(
          final String noteId, final String paragraphId, final int outputIndex, final String output) {
    try {
      callRemoteFunction(client -> {
        client.appendOutput(
                new OutputAppendEvent(noteId, paragraphId, outputIndex, output, null));
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to appendOutput", e);
    }
  }

  @Override
  public void onInterpreterOutputUpdate(
          final String noteId, final String paragraphId, final int outputIndex,
          final InterpreterResult.Type type, final String output) {
    try {
      callRemoteFunction(client -> {
        client.updateOutput(
                new OutputUpdateEvent(noteId, paragraphId, outputIndex, type.name(), output, null));
        return null;
      });

    } catch (Exception e) {
      LOGGER.warn("Fail to updateOutput", e);
    }
  }

  @Override
  public void onInterpreterOutputUpdateAll(
          final String noteId, final String paragraphId, final List<InterpreterResultMessage> messages) {
    try {
      callRemoteFunction(client -> {
        client.updateAllOutput(
                new OutputUpdateAllEvent(noteId, paragraphId, convertToThrift(messages)));
        return null;
      });

    } catch (Exception e) {
      LOGGER.warn("Fail to updateAllOutput", e);
    }
  }

  private List<com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterResultMessage>
        convertToThrift(List<InterpreterResultMessage> messages) {
    List<com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterResultMessage> thriftMessages =
        new ArrayList<>();
    for (InterpreterResultMessage message : messages) {
      thriftMessages.add(
          new com.teragrep.zep_01.interpreter.thrift.RemoteInterpreterResultMessage(
              message.getType().name(), message.getData()));
    }
    return thriftMessages;
  }

  @Override
  public void runParagraphs(String noteId,
                            final List<String> paragraphIds,
                            final List<Integer> paragraphIndices,
                            final String curParagraphId) {
    RunParagraphsEvent event =
        new RunParagraphsEvent(noteId, paragraphIds, paragraphIndices, curParagraphId);
    try {
      callRemoteFunction(client -> {
        client.runParagraphs(event);
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to runParagraphs: {}", event, e);
    }
  }

  @Override
  public void checkpointOutput(String noteId, String paragraphId) {
    try {
      callRemoteFunction(client -> {
        client.checkpointOutput(noteId, paragraphId);
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to checkpointOutput of paragraph: {} of note: {}",
              paragraphId, noteId, e);
    }
  }

  @Override
  public void onParaInfosReceived(Map<String, String> infos) {
    try {
      callRemoteFunction(client -> {
        client.sendParagraphInfo(intpGroupId, GSON.toJson(infos));
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to onParaInfosReceived: {}", infos, e);
    }
  }

  @Override
  public synchronized void onAddAngularObject(String interpreterGroupId, AngularObject angularObject) {
    try {
      callRemoteFunction(client -> {
        client.addAngularObject(intpGroupId, angularObject.toJson());
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to add AngularObject: {}", angularObject, e);
    }
  }

  @Override
  public void onUpdateAngularObject(String interpreterGroupId, AngularObject angularObject) {
    try {
      callRemoteFunction(client -> {
        client.updateAngularObject(intpGroupId, angularObject.toJson());
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to update AngularObject: {}", angularObject, e);
    }
  }

  @Override
  public void onRemoveAngularObject(String interpreterGroupId, AngularObject angularObject) {
    try {
      callRemoteFunction(client -> {
        client.removeAngularObject(intpGroupId,
                angularObject.getNoteId(),
                angularObject.getParagraphId(),
                angularObject.getName());
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to remove AngularObject", e);
    }
  }

  @Override
  public void updateParagraphConfig(String noteId, String paragraphId, Map<String, String> config) {
    try {
      callRemoteFunction(client -> {
        client.updateParagraphConfig(noteId, paragraphId, config);
        return null;
      });
    } catch (Exception e) {
      LOGGER.warn("Fail to updateParagraphConfig", e);
    }
  }

  @Override
  public void close() {
    remoteClient.close();
  }
}
