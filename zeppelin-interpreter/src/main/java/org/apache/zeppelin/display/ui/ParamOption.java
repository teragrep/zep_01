package org.apache.zeppelin.display.ui;

public interface ParamOption {

    Object getValue();

    void setValue(Object value);

    String getDisplayName();

    void setDisplayName(String displayName);

}
