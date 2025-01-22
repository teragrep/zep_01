package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.display.ui.ParamOption;
import org.apache.zeppelin.interpreter.xref.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.xref.display.GUI;
import org.apache.zeppelin.interpreter.xref.annotation.Experimental;
import org.apache.zeppelin.interpreter.xref.annotation.ZeppelinApi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ZeppelinContext {

    // Map interpreter class name (to be used by hook registry) from
    // given replName in paragraph
    Map<String, String> getInterpreterClassMap();

    List<Class> getSupportedClasses();

    int getMaxResult();

    String showData(Object obj);

    /**
     * subclasses should implement this method to display specific data type
     *
     * @param obj
     * @param maxResult max number of rows to display
     * @return
     */
    String showData(Object obj, int maxResult);

    @Deprecated
    @ZeppelinApi
    Object input(String name);

    @Deprecated
    @ZeppelinApi
    Object input(String name, Object defaultValue);

    @ZeppelinApi
    Object textbox(String name);

    @ZeppelinApi
    Object textbox(String name, String defaultValue);

    @ZeppelinApi
    Object noteTextbox(String name);

    @ZeppelinApi
    Object noteTextbox(String name, String defaultValue);

    @ZeppelinApi
    Object password(String name);

    @ZeppelinApi
    Object notePassword(String name);

    @ZeppelinApi
    List<Object> checkbox(String name, ParamOption[] options);

    @ZeppelinApi
    List<Object> checkbox(
            String name, ParamOption[] options, List defaultChecked
    );

    @Deprecated
    @ZeppelinApi
    List<Object> checkbox(
            String name, List<Object> defaultChecked, ParamOption[] options
    );

    @ZeppelinApi
    List<Object> noteCheckbox(String name, ParamOption[] options);

    @Deprecated
    @ZeppelinApi
    List<Object> noteCheckbox(
            String name, List<Object> defaultChecked, ParamOption[] options
    );

    @ZeppelinApi
    List<Object> noteCheckbox(
            String name, ParamOption[] options, List defaultChecked
    );

    @ZeppelinApi
    Object select(String name, ParamOption[] paramOptions);

    @Deprecated
    @ZeppelinApi
    Object select(String name, Object defaultValue, ParamOption[] paramOptions);

    @ZeppelinApi
    Object select(String name, ParamOption[] paramOptions, Object defaultValue);

    @ZeppelinApi
    Object noteSelect(String name, ParamOption[] paramOptions);

    @Deprecated
    @ZeppelinApi
    Object noteSelect(String name, Object defaultValue, ParamOption[] paramOptions);

    @ZeppelinApi
    Object noteSelect(String name, ParamOption[] paramOptions, Object defaultValue);

    void setGui(GUI o);

    GUI getGui();

    GUI getNoteGui();

    void setNoteGui(GUI noteGui);

    InterpreterContext getInterpreterContext();

    void setInterpreterContext(InterpreterContext interpreterContextImpl);

    void setMaxResult(int maxResult);

    @ZeppelinApi
    void show(Object o);

    @ZeppelinApi
    void show(Object o, int maxResult);

    @ZeppelinApi
    void run(String paragraphId) throws IOException;

    @ZeppelinApi
    void run(String paragraphId, boolean checkCurrentParagraph) throws IOException;

    @ZeppelinApi
    void run(String noteId, String paragraphId) throws IOException;

    void run(String noteId, String paragraphId, InterpreterContext context) throws IOException;

    @ZeppelinApi
    void runNote(String noteId) throws IOException;

    void runNote(String noteId, InterpreterContext context) throws IOException;

    @ZeppelinApi
    void run(int idx) throws IOException;

    @ZeppelinApi
    void run(int idx, boolean checkCurrentParagraph) throws IOException;

    @ZeppelinApi
    void runAll() throws IOException;

    void runAll(InterpreterContext context) throws IOException;

    @ZeppelinApi
    Object angular(String name);

    Object angular(String name, String noteId);

    Object angular(String name, String noteId, String paragraphId);

    @Deprecated
    Object angularGlobal(String name);

    @ZeppelinApi
    void angularBind(String name, Object o);

    @Deprecated
    void angularBindGlobal(String name, Object o);

    @ZeppelinApi
    void angularBind(String name, Object o, AngularObjectWatcher watcher);

    @Deprecated
    void angularBindGlobal(String name, Object o, AngularObjectWatcher watcher);

    @ZeppelinApi
    void angularWatch(String name, AngularObjectWatcher watcher);

    @Deprecated
    void angularWatchGlobal(String name, AngularObjectWatcher watcher);

    @ZeppelinApi
    void angularUnwatch(String name, AngularObjectWatcher watcher);

    @Deprecated
    void angularUnwatchGlobal(String name, AngularObjectWatcher watcher);

    @ZeppelinApi
    void angularUnwatch(String name);

    @Deprecated
    void angularUnwatchGlobal(String name);

    @ZeppelinApi
    void angularUnbind(String name);

    @Deprecated
    void angularUnbindGlobal(String name);

    void angularBind(String name, Object o, String noteId);

    void angularBind(String name, Object o, String noteId, String paragraphId);

    void angularWatch(String name, String noteId, AngularObjectWatcher watcher);

    @Experimental
    void registerHook(String event, String cmd, String replName) throws InvalidHookException;

    @Experimental
    void registerHook(String event, String cmd) throws InvalidHookException;

    @Experimental
    void registerNoteHook(String event, String cmd, String noteId) throws InvalidHookException;

    @Experimental
    void registerNoteHook(String event, String cmd, String noteId, String replName) throws InvalidHookException;

    @Experimental
    void unregisterHook(String event, String replName);

    @Experimental
    void unregisterHook(String event);

    @Experimental
    void unregisterNoteHook(String noteId, String event);

    @Experimental
    void unregisterNoteHook(String noteId, String event, String replName);

    @ZeppelinApi
    void put(String name, Object value);

    @ZeppelinApi
    Object get(String name);

    @ZeppelinApi
    <T> T get(String name, Class<T> clazz);

    @ZeppelinApi
    void remove(String name);

    @ZeppelinApi
    boolean containsKey(String name);

    @ZeppelinApi
    ResourceSet getAll();

}
