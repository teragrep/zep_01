package org.apache.zeppelin.display;

import org.apache.zeppelin.common.JsonSerializable;

public interface AngularObject<T> extends JsonSerializable {

    String getName();

    void setNoteId(String noteId);

    String getNoteId();

    String getParagraphId();

    void setParagraphId(String paragraphId);

    boolean isGlobal();

    Object get();

    void emit();

    void set(T o);

    void set(T o, boolean emit);

    void setListener(AngularObjectListener listener);

    AngularObjectListener getListener();

    void addWatcher(AngularObjectWatcher watcher);

    void removeWatcher(AngularObjectWatcher watcher);

    void clearAllWatchers();

}
