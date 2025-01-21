package org.apache.zeppelin.interpreter.xref;

/**
 * Type of result after code execution.
 */
public enum Code {
    SUCCESS, INCOMPLETE, ERROR, KEEP_PREVIOUS_RESULT
}
