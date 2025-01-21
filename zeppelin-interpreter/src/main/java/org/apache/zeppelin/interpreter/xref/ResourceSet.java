package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.common.JsonSerializable;

import java.util.List;

public interface ResourceSet extends List<Resource>, JsonSerializable {

    ResourceSet filterByNameRegex(String regex);

    ResourceSet filterByName(String name);

    ResourceSet filterByClassnameRegex(String regex);

    ResourceSet filterByClassname(String className);

    ResourceSet filterByNoteId(String noteId);

    ResourceSet filterByParagraphId(String paragraphId);

}
