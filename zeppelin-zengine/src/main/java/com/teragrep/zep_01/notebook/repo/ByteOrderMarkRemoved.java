package com.teragrep.zep_01.notebook.repo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// String decorator that removes an UTF-8 Byte order mark from the start of the string if it exists.
public final class ByteOrderMarkRemoved {
    final String origin;
    public ByteOrderMarkRemoved(String origin){
        this.origin = origin;
    }
    public String toString(){
        if (origin.toString().startsWith("\uFEFF")) {
            return origin.toString().substring(1);
        }
        return origin.toString();
    }
}
