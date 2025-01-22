package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.user.AuthenticationInfo;

import java.util.Map;

public interface InterpreterContext {

    String getNoteId();

    String getNoteName();

    String getReplName();

    String getParagraphId();

    void setParagraphId(String paragraphId);

    String getParagraphText();

    String getParagraphTitle();

    Map<String, String> getLocalProperties();

    String getStringLocalProperty(String key, String defaultValue);

    int getIntLocalProperty(String key, int defaultValue);

    long getLongLocalProperty(String key, int defaultValue);

    double getDoubleLocalProperty(String key, double defaultValue);

    boolean getBooleanLocalProperty(String key, boolean defaultValue);

    AuthenticationInfo getAuthenticationInfo();

    Map<String, Object> getConfig();

    GUI getGui();

    GUI getNoteGui();

    AngularObjectRegistry getAngularObjectRegistry();

    void setAngularObjectRegistry(AngularObjectRegistry angularObjectRegistry);

    ResourcePool getResourcePool();

    void setResourcePool(ResourcePool resourcePool);

    String getInterpreterClassName();

    void setInterpreterClassName(String className);

    RemoteInterpreterEventClient getIntpEventClient();

    void setIntpEventClient(RemoteInterpreterEventClient intpEventClient);

    InterpreterOutput out();

    void setProgress(int n);

}
