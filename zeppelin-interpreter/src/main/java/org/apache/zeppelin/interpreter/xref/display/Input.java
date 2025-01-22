package org.apache.zeppelin.interpreter.xref.display;

import java.io.Serializable;

public interface Input<T> extends Serializable {

    boolean isHidden();

    String getName();

    T getDefaultValue();

    String getDisplayName();

    void setDisplayName(String displayName);

    void setArgument(String argument);

    void setHidden(boolean hidden);

    String getArgument();

}
