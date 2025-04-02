package com.teragrep.zep_01.notebook.repo;

import com.teragrep.zep_01.notebook.NoteInfo;
import com.teragrep.zep_01.notebook.utility.IdHashes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class Directory extends ZeppelinFile {
    private final Map<String, ZeppelinFile> children;

    public Directory(String id, Path path){
        this(id,path,new HashMap<>());
    }
    public Directory(String id, Path path, Map<String, ZeppelinFile> children){
        super(id,path);
        this.children = Collections.unmodifiableMap(children);
    }

    // Find a matching ZeppelinFile by ID
    public ZeppelinFile findFile(String id) throws FileNotFoundException {
        if(id().equals(id)){
            return this;
        }
        else {
            for (ZeppelinFile child: children.values()){
                try{
                    return(child.findFile(id));
                }catch (FileNotFoundException exception){
                    continue;
                }
            }
            throw new FileNotFoundException("Notebook or directory "+id+" not found!");
        }
    }

    // Find a matching ZeppelinFile by Path
    public ZeppelinFile findFile(Path path) throws FileNotFoundException {
        if(path().equals(path)){
            return this;
        }
        else {
            for (ZeppelinFile child: children.values()){
                try{
                    return(child.findFile(path));
                }catch (FileNotFoundException exception){
                    continue;
                }
            }
            throw new FileNotFoundException("Notebook or directory with path "+path.toString()+" not found!");
        }
    }

    public void move(Path destination) throws IOException{
        if(destination.toAbsolutePath().startsWith(path().toAbsolutePath())){
            throw new IOException("Cannot move a directory into one of its own children!");
        }
        HashMap<String, ZeppelinFile> movedChildren = new HashMap<>();
        for (ZeppelinFile child : children.values()) {
            if(child.isStub()){
                child = child.load();
            }
            ZeppelinFile movedChild = child.copy(Paths.get(destination.toString(),child.path().getFileName().toString()),child.id());
            movedChildren.put(movedChild.id(),movedChild);
        }

        Directory movedDirectory = new Directory(id(),destination,movedChildren);
        movedDirectory.save();
        delete();
    }
    public void move(Directory destinationDirectory, String name) throws IOException{
        move(Paths.get(destinationDirectory.path().toString(),name));
    }
    public void delete() throws IOException{
        for (ZeppelinFile child: children.values()){
            child.delete();
        }
        Files.delete(path());
    }
    public Directory copy(Path path,String id) throws IOException {
        HashMap<String,ZeppelinFile> copyChildren = new HashMap<>();
        for (ZeppelinFile child: children.values()) {
            if(child.isStub()){
                child = child.load();
            }
            String copyId = IdHashes.generateId();
            String childFileName = child.path().getFileName().toString();
            StringBuilder sb = new StringBuilder(childFileName);
            sb.replace(childFileName.lastIndexOf("_")+1
                    ,(childFileName.endsWith(".zpln") ? childFileName.lastIndexOf(".zpln") : childFileName.length())
                    , copyId);
            Path copyChildPath = Paths.get(path.toString(),sb.toString());
            copyChildren.put(copyId,child.copy(copyChildPath,copyId));
        }
        Directory copiedDirectory = new Directory(id,path,copyChildren);
        copiedDirectory.save();
        return copiedDirectory;
    }

    public Map<String, ZeppelinFile> children(){
        return children;
    }

    public List<ZeppelinFile> listAllChildren(){
        ArrayList<ZeppelinFile> allChildren = new ArrayList<ZeppelinFile>();
        for (ZeppelinFile child:children.values()) {
            allChildren.add(child);
            allChildren.addAll(child.listAllChildren());
        }
        return allChildren;
    }

    public HashMap<String,NoteInfo> toNoteInfo(HashMap<String,NoteInfo> noteInfoMap, Path rootDir){
        // noteInfoMap.put(id,new FormattedNoteInfo(id,path.toString())); // Uncomment this to include Directory itself in the NoteList. For now including this causes errors in at least SearchService, as it's expecting to find only Note objects.
        for (ZeppelinFile child: children.values()) {
                noteInfoMap.putAll(child.toNoteInfo(noteInfoMap,rootDir));
        }
        return noteInfoMap;
    }
    public void save() throws IOException{
        if(!Files.exists(path())){
            Files.createDirectory(path());
        }
        for (ZeppelinFile child:children.values()) {
            child.save();
        }
    }
    public void rename(String fileName) throws IOException{
        move(Paths.get(path().getParent().toString(),fileName));
    }
    public  boolean isDirectory(){
        return true;
    }

    public void printTree(){
        System.out.println("Dir; Id: "+id()+", Path: "+path());
        for (Map.Entry<String, ZeppelinFile> child: children.entrySet()) {
            child.getValue().printTree();
        }
    }
    // Directories don't require any operation for lazy loading
    public Directory load() throws IOException{
        return this;
    }

    public String readFile() throws IOException{
        throw new IOException("Cannot read a file that is a directory!");
    }

    public boolean isStub(){
        return false;
    }
}
