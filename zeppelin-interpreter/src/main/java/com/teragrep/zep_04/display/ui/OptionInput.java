package com.teragrep.zep_04.display.ui;

import com.teragrep.zep_04.display.Input;

public interface OptionInput<T> extends Input<T> {

    ParamOption[] getOptions();

}
