package org.apache.zeppelin.interpreter.xref.display.ui;

import org.apache.zeppelin.interpreter.xref.display.Input;

public interface OptionInput<T> extends Input<T> {

    ParamOption[] getOptions();

}
