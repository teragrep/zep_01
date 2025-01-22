package org.apache.zeppelin.interpreter.xref.display;

import org.apache.zeppelin.display.ui.ParamOptionImpl;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface GUI extends Serializable {

    void setParams(Map<String, Object> values);

    Map<String, Object> getParams();

    Map<String, Input> getForms();

    void setForms(Map<String, Input> forms);

    @Deprecated
    Object input(String id);

    @Deprecated
    Object input(String id, Object defaultValue);

    Object textbox(String id, String defaultValue);

    Object textbox(String id);

    Object password(String id);

    Object select(String id, ParamOptionImpl[] options, Object defaultValue);

    List<Object> checkbox(
            String id, ParamOptionImpl[] options, Collection<Object> defaultChecked
    );

    void clear();

    String toJson();

    void convertOldInput();

}
