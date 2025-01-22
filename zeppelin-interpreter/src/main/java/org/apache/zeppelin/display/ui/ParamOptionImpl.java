package org.apache.zeppelin.display.ui;

import org.apache.zeppelin.interpreter.xref.display.ui.ParamOption;

/**
 * Parameters option.
 */
public class ParamOptionImpl implements ParamOption {

    Object value;
    String displayName;

    public ParamOptionImpl(Object value, String displayName) {
        super();
        this.value = value;
        this.displayName = displayName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParamOptionImpl that = (ParamOptionImpl) o;

        if (value != null ? !value.equals(that.value) : that.value != null)
            return false;
        return displayName != null ? displayName.equals(that.displayName) : that.displayName == null;

    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
        return result;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

}
