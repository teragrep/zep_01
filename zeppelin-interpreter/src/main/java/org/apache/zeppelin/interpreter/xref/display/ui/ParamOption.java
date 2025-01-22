package org.apache.zeppelin.interpreter.xref.display.ui;

public interface ParamOption {

    Object getValue();

    void setValue(Object value);

    String getDisplayName();

    void setDisplayName(String displayName);

}
