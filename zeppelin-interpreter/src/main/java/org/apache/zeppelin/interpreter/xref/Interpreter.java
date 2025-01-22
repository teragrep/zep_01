package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultImpl;
import org.apache.zeppelin.interpreter.xref.annotation.Experimental;
import org.apache.zeppelin.interpreter.xref.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;

import java.net.URL;
import java.util.List;
import java.util.Properties;

public interface Interpreter {

    /**
     * Opens interpreter. You may want to place your initialize routine here. open() is called only once
     */
    @ZeppelinApi
    void open() throws InterpreterException;

    /**
     * Closes interpreter. You may want to free your resources up here. close() is called only once
     */
    @ZeppelinApi
    void close() throws InterpreterException;

    @ZeppelinApi
    InterpreterResult executePrecode(InterpreterContext interpreterContext) throws InterpreterException;

    /**
     * Run code and return result, in synchronous way.
     *
     * @param st statements to run
     */
    @ZeppelinApi
    InterpreterResultImpl interpret(
            String st, InterpreterContext context
    ) throws InterpreterException;

    /**
     * Optionally implement the canceling routine to abort interpret() method
     */
    @ZeppelinApi
    void cancel(InterpreterContext context) throws InterpreterException;

    /**
     * Dynamic form handling see http://zeppelin.apache.org/docs/dynamicform.html
     *
     * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}), FormType.NATIVE handles
     * form in API
     */
    @ZeppelinApi
    FormType getFormType() throws InterpreterException;

    /**
     * get interpret() method running process in percentage.
     *
     * @return number between 0-100
     */
    @ZeppelinApi
    int getProgress(InterpreterContext context) throws InterpreterException;

    @ZeppelinApi
    List<InterpreterCompletion> completion(
            String buf, int cursor, InterpreterContext interpreterContextImpl
    ) throws InterpreterException;

    @ZeppelinApi
    Scheduler getScheduler();

    void setProperties(Properties properties);

    @ZeppelinApi
    Properties getProperties();

    @ZeppelinApi
    String getProperty(String key);

    @ZeppelinApi
    String getProperty(String key, String defaultValue);

    @ZeppelinApi
    void setProperty(String key, String value);

    String getClassName();

    void setUserName(String userName);

    String getUserName();

    void setInterpreterGroup(InterpreterGroup interpreterGroup);

    @ZeppelinApi
    InterpreterGroup getInterpreterGroup();

    URL[] getClassloaderUrls();

    void setClassloaderUrls(URL[] classloaderUrls);

    @Experimental
    void registerHook(String noteId, String event, String cmd) throws InvalidHookException;

    @Experimental
    void registerHook(String event, String cmd) throws InvalidHookException;

    @Experimental
    String getHook(String noteId, String event);

    @Experimental
    String getHook(String event);

    @Experimental
    void unregisterHook(String noteId, String event);

    @Experimental
    void unregisterHook(String event);

    @ZeppelinApi
    <T> T getInterpreterInTheSameSessionByClassName(Class<T> interpreterClass, boolean open)
            throws InterpreterException;

    <T> T getInterpreterInTheSameSessionByClassName(Class<T> interpreterClass) throws InterpreterException;

}
