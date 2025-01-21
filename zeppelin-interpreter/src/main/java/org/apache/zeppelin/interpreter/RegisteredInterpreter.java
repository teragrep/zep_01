package org.apache.zeppelin.interpreter;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent registered interpreter class
 */
public class RegisteredInterpreter {

    private final String group;
    private final String name;
    private final String className;
    private boolean defaultInterpreter;
    private final Map<String, DefaultInterpreterProperty> properties;
    private final Map<String, Object> editor;
    private Map<String, Object> config;
    private String path;
    private InterpreterOption option;
    private InterpreterRunner runner;

    public RegisteredInterpreter(
            String name, String group, String className, Map<String, DefaultInterpreterProperty> properties
    ) {
        this(name, group, className, false, properties);
    }

    public RegisteredInterpreter(
            String name,
            String group,
            String className,
            boolean defaultInterpreter,
            Map<String, DefaultInterpreterProperty> properties
    ) {
        super();
        this.name = name;
        this.group = group;
        this.className = className;
        this.defaultInterpreter = defaultInterpreter;
        this.properties = properties;
        this.editor = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getGroup() {
        return group;
    }

    public String getClassName() {
        return className;
    }

    public boolean isDefaultInterpreter() {
        return defaultInterpreter;
    }

    public void setDefaultInterpreter(boolean defaultInterpreter) {
        this.defaultInterpreter = defaultInterpreter;
    }

    public Map<String, DefaultInterpreterProperty> getProperties() {
        return properties;
    }

    public Map<String, Object> getEditor() {
        return editor;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public String getInterpreterKey() {
        return getGroup() + "." + getName();
    }

    public InterpreterOption getOption() {
        return option;
    }

    public InterpreterRunner getRunner() {
        return runner;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

}
