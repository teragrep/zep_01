package com.teragrep.zep_01.notebook.repo;

import com.teragrep.zep_01.notebook.Stubable;
import com.teragrep.zep_01.notebook.NoteInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ZeppelinFile implements Stubable {
    private final String id;
    private final Path path;
    public ZeppelinFile(String id, Path path){
        this.id = id;
        this.path = path;
    }
    public abstract void delete() throws IOException;
    public abstract ZeppelinFile findFile(String id) throws FileNotFoundException;
    public abstract ZeppelinFile findFile(Path path) throws FileNotFoundException;
    public String id(){
        return id;
    };
    public Path path(){
        return path;
    }
    public abstract void save() throws IOException;
    public abstract boolean isDirectory();
    /**
     * Copies a ZeppelinFile in memory, assigning it a new ID
     * @param path
     * @param id
     * @return
     * @throws IOException
     */
    public abstract ZeppelinFile copy(Path path, String id) throws IOException;
    public abstract Map<String,ZeppelinFile> children();
    public abstract Map<String,NoteInfo> toNoteInfo(HashMap<String,NoteInfo> noteInfoMap, Path rootDir);
    public abstract void printTree();
    public abstract String readFile() throws IOException;
    public abstract ZeppelinFile load() throws IOException;
    public abstract void move(Path path) throws IOException;
    public abstract void rename(String name) throws IOException;
    public abstract List<ZeppelinFile> listAllChildren();
}
