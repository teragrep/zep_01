package org.apache.zeppelin.display.ui;

import org.apache.zeppelin.interpreter.xref.display.Input;
import org.apache.zeppelin.interpreter.xref.display.ui.ParamOption;

public interface OptionInput<T> extends Input<T> {

    ParamOption[] getOptions();

}
