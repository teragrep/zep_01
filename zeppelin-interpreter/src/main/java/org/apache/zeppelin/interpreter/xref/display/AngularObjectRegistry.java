package org.apache.zeppelin.interpreter.xref.display;

import java.util.List;
import java.util.Map;

public interface AngularObjectRegistry {

    AngularObjectRegistryListener getListener();

    AngularObject add(String name, Object o, String noteId, String paragraphId);

    AngularObject add(
            String name, Object o, String noteId, String paragraphId, boolean emit
    );

    AngularObject remove(String name, String noteId, String paragraphId);

    AngularObject remove(String name, String noteId, String paragraphId, boolean emit);

    void removeAll(String noteId, String paragraphId);

    AngularObject get(String name, String noteId, String paragraphId);

    List<AngularObject> getAll(String noteId, String paragraphId);

    List<AngularObject> getAllWithGlobal(String noteId);

    String getInterpreterGroupId();

    Map<String, Map<String, AngularObject>> getRegistry();

    void setRegistry(Map<String, Map<String, AngularObject>> registry);

}
