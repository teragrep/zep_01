package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.common.JsonSerializable;

public interface ResourceId extends JsonSerializable {

    String getResourcePoolId();

    String getName();

    String getNoteId();

    String getParagraphId();

}
