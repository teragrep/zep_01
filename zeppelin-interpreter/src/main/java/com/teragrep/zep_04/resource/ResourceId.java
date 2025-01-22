package com.teragrep.zep_04.resource;

import org.apache.zeppelin.common.JsonSerializable;

public interface ResourceId extends JsonSerializable {

    String getResourcePoolId();

    String getName();

    String getNoteId();

    String getParagraphId();

}
