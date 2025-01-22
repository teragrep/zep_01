package com.teragrep.zep_04.interpreter;

import com.teragrep.zep_04.resource.ResourcePool;
import com.teragrep.zep_04.display.AngularObjectRegistry;
import com.teragrep.zep_04.display.GUI;
import com.teragrep.zep_04.remote.RemoteInterpreterEventClient;
import com.teragrep.zep_04.user.AuthenticationInfo;

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
