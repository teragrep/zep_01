package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.interpreter.xref.Interpreter;
import org.apache.zeppelin.interpreter.xref.ResourcePool;
import org.apache.zeppelin.interpreter.xref.display.AngularObjectRegistry;

import java.util.Collection;
import java.util.List;

public interface InterpreterGroup {

    String getId();

    String getWebUrl();

    void setWebUrl(String webUrl);

    //TODO(zjffdu) change it to getSession. For now just keep this method to reduce code change
    List<Interpreter> get(String sessionId);

    //TODO(zjffdu) change it to addSession. For now just keep this method to reduce code change
    void put(String sessionId, List<Interpreter> interpreters);

    void addInterpreterToSession(Interpreter interpreter, String sessionId);

    //TODO(zjffdu) rename it to a more proper name.
    //For now just keep this method to reduce code change
    Collection<List<Interpreter>> values();

    AngularObjectRegistry getAngularObjectRegistry();

    void setAngularObjectRegistry(AngularObjectRegistry angularObjectRegistry);

    InterpreterHookRegistry getInterpreterHookRegistry();

    void setInterpreterHookRegistry(InterpreterHookRegistry hookRegistry);

    int getSessionNum();

    void setResourcePool(ResourcePool resourcePool);

    ResourcePool getResourcePool();

    boolean isAngularRegistryPushed();

    void setAngularRegistryPushed(boolean angularRegistryPushed);

    boolean isEmpty();

    void close();

}
