package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.interpreter.ZeppelinContext;

public interface EnhancedInterpreter extends Interpreter {

    ZeppelinContext getZeppelinContext();

}
