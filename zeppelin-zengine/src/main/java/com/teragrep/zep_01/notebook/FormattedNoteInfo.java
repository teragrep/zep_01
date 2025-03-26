package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.notebook.repo.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of NoteInfo that formats Notebook file names from new Paths for listing to the UI.
 */
public class FormattedNoteInfo extends NoteInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(Directory.class);

    public FormattedNoteInfo(String id, String path) {
        super(id, path);
    }

    @Override
    public String getPath() {
        int endpos = super.getPath().lastIndexOf("_");
        if(endpos == -1){
            LOGGER.warn("info.path is malformed! {}",super.getPath());
            return super.getPath();
        }
        else {
            return super.getPath().substring(0,endpos);
        }
    }
    @Override
    public String getNoteName() {
        int startpos = super.getPath().lastIndexOf("/") + 1;
        int endpos = super.getPath().lastIndexOf("_");
        if(startpos == -1 || endpos == -1){
            LOGGER.warn("info.name is malformed! {}",super.getPath());
            return super.getPath();
        }
        else {
            return super.getPath().substring(startpos,endpos);
        }
    }
    @Override
    public String getParent() {
        int pos = super.getPath().lastIndexOf("/");
        return super.getPath().substring(0, pos);
    }
}
