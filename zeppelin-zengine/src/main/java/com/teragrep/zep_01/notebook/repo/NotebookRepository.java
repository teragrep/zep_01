package com.teragrep.zep_01.notebook.repo;

import com.teragrep.zep_01.notebook.*;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.*;
import com.teragrep.zep_01.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// This is a middleware that enables communication between Legacy Zeppelin and the new filesystem
public class NotebookRepository implements NotebookRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotebookRepository.class);
    private FileSystem fileSystem;
    private Directory root;
    private Path rootDir;

    // This gets called by HK2 dependency injection framework In ZeppelinServer.java when Zeppelin is started.
    public NotebookRepository() throws IOException {
        init(ZeppelinConfiguration.create());
    }

    // Init() creates a file tree structure based on existing files within NotebookDir defined in ZeppelinConfiguration.
    @Override
    public void init(ZeppelinConfiguration zConf) throws IOException {
        fileSystem = FileSystems.getDefault();
        rootDir = fileSystem.getPath(zConf.getNotebookDir()).normalize();

        if(!Files.exists(rootDir)){
            Files.createDirectories(rootDir);
        }
        root = createRootDirectory(new ConcurrentHashMap<>());
    }

    private Directory createRootDirectory(Map<String,ZeppelinFile> existingFiles) throws IOException{
        try{
            return updateFileTreeFromFiles(rootDir,existingFiles);
        }catch (Exception exception){
            throw new IOException("Failed to initialize NotebookRepository!" ,exception);
        }
    }
    private String extractIDFromFileName(Path file) {
        return extractIDFromFileName(file,"_");
    }

    private String extractIDFromFileName(Path file,String delimiter) {
        String fileName = file.getFileName().toString();
        if(fileName.contains(delimiter)){
            int idStartIndex = fileName.lastIndexOf(delimiter);
            if(fileName.endsWith(".zpln")){
                int idEndIndex = fileName.lastIndexOf(".zpln");
                return fileName.substring(idStartIndex+1,idEndIndex);
            }
            return fileName.substring(idStartIndex+1);
        }
        // If filename doesn't conform to naming conventions, return the filename iteself.
        else {
            return fileName;
        }
    }


    public Directory updateFileTreeFromFiles(Path path, Map<String,ZeppelinFile> existingFiles) throws IOException {
        // Create a copy of existingFiles so that we don't make any direct edits to it.
        HashMap<String,ZeppelinFile> children = new HashMap<>();
        children.putAll(existingFiles);
        Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                // walkFileTree visits the root directory it's called on.
                // This method operates on the given directory's children so we skip the processing of the root directory here.
                if(dir.equals(path)){
                    return FileVisitResult.CONTINUE;
                }
                // Ignore .git directory. This could be moved somewhere else.
                if(dir.startsWith(path+"/.git")){
                    return FileVisitResult.SKIP_SUBTREE;
                }
                // Read an ID from the file name
                String directoryId = extractIDFromFileName(dir);
                    try {
                        // Here we create any child Directories by calling this function recursively.
                        // First we will check if we already have the files of the child Directory in existingFiles, and pass them to the recursive call so that we don't do any unnecessary operations in later recursions.
                        ConcurrentHashMap<String,ZeppelinFile> subtreeChildren = new ConcurrentHashMap<>();
                        if(children.containsKey(directoryId)){
                            subtreeChildren.putAll(children.get(directoryId).children());
                        }
                        Directory subtree = updateFileTreeFromFiles(dir,subtreeChildren);
                        children.put(subtree.id(),subtree);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String notebookId = extractIDFromFileName(file);
                if(!children.containsKey(notebookId)){
                    children.put(notebookId,new UnloadedNotebook(notebookId,file));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return super.visitFileFailed(file, exc);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return super.postVisitDirectory(dir, exc);
            }
        });
        Directory root = new Directory(extractIDFromFileName(path),path,children);
        return root;
    }
    /**
     * Lists notebook information about all notebooks in storage. This method should only read
     * the note file name, rather than reading all note content which usually takes long time.
     *
     * @param subject contains user information.
     * @return Map of noteId -> NoteInfo
     * @throws IOException
     */
    @Override
    public Map<String, NoteInfo> list(AuthenticationInfo subject) throws IOException {
        root = createRootDirectory(root.children());
        HashMap<String,NoteInfo> notelist = new HashMap<>();
        for (ZeppelinFile node:root.children().values()) {
                notelist.putAll(node.toNoteInfo(notelist,rootDir));
        }
        return notelist;
    }

    /**
     * Get the notebook with the given noteId and given notePath.
     *
     * @param noteId   is note id.
     * @param notePath is note path
     * @param subject  contains user information.
     * @return
     * @throws IOException
     */
    @Override
    public Note get(String noteId, String notePath, AuthenticationInfo subject) throws IOException {
        try{
            ZeppelinFile notebook = root.findFile(noteId);
            notebook = notebook.load();

            Map<String, ZeppelinFile> children = new HashMap<>();
            children.putAll(root.children());
            children.put(noteId,notebook);
            root = createRootDirectory(children);
            Note note = Note.fromJson(noteId,notebook.readFile());
            if(notePath == ""){                                                 // Sometimes Zeppelin calls notebookrepo.get() with an empty notePath, such as in LuceneSearch, leading to incorrect path values, so we need to handle that case until it's refactored
                note.setPath(rootDir.relativize(notebook.path()).toString());        // Setting path here ensures that note does not have a "path" variable as null, which causes errors
            }
            else{
                note.setPath(notePath);
            }
            return note;
        }catch (Exception exception){
            throw new IOException("Failed to create a Note from JSON!",exception);
        }
    }

    public void save(String notebookId, AuthenticationInfo subject) throws IOException {
            ZeppelinFile notebook = root.findFile(notebookId);
            if(!notebook.isStub()){
                notebook.save();
            }
            root = createRootDirectory(root.children());
    }
    /**
     * Save given note in storage
     *
     * @param note    is the note itself.
     * @param subject contains user information.
     * @throws IOException
     */
    @Override
    public void save(Note note, AuthenticationInfo subject) throws IOException {
        try{
            Notebook notebook = note.toNotebook();
            notebook.save();
            root = createRootDirectory(root.children());
        }catch (Exception exception){
            throw new IOException("Failed to save note!",exception);
        }
    }

    /**
     * Move given note to another path
     *
     * @param noteId
     * @param subject
     * @throws IOException
     */
    @Override
    public void move(String noteId, String newName, String folderPath, AuthenticationInfo subject) throws IOException{
        try{
            Path destinationPath = Paths.get(folderPath,newName+"_"+noteId+".zpln");
            ZeppelinFile source = root.findFile(noteId);
            source.move(destinationPath);
            root = createRootDirectory(root.children());
        } catch (Exception exception){
            throw new IOException("Failed to move object!",exception);
        }
    }
    /**
     * Move folder to another path
     *
     * @param folderId
     * @param newFolderPath
     * @param subject
     * @throws IOException
     */
    public void move(String folderId, Path newFolderPath, AuthenticationInfo subject) throws IOException {
        try{
            ZeppelinFile directory = root.findFile(folderId);
            directory.move(newFolderPath);
            root = createRootDirectory(root.children());
        }catch (Exception exception){
            throw new IOException("Failed to move notebook!",exception);
        }
    }

    /**
     * Legacy method for moving folder to another path
     *
     * @param folderId
     * @param newFolderPath
     * @param subject
     * @throws IOException
     */
    @Override
    public void move(String folderId, String newFolderPath, AuthenticationInfo subject) throws IOException {
        move(folderId,Paths.get(newFolderPath),subject);
    }

    /**
     * Remove note with given id and notePath
     *
     * @param noteId   is note id.
     * @param notePath is note path
     * @param subject  contains user information.
     * @throws IOException
     */
    @Override
    public void remove(String noteId, String notePath, AuthenticationInfo subject) throws IOException {
        try{
            ZeppelinFile toRemove = root.findFile(noteId);
            toRemove.delete();
            ConcurrentHashMap<String, ZeppelinFile> remainingChildren = new ConcurrentHashMap<>();
            remainingChildren.putAll(root.children());
            remainingChildren.remove(noteId);
            root = createRootDirectory(remainingChildren);
        }catch (Exception exception){
            throw new IOException("Failed to remove note!",exception);
        }
    }

    /**
     * Remove folder
     *
     * @param folderPath
     * @param subject
     * @throws IOException
     */
    @Override
    public void remove(String folderPath, AuthenticationInfo subject) throws IOException {
        try{
            ZeppelinFile toRemove = root.findFile(folderPath);
            toRemove.delete();
            root = createRootDirectory(root.children());
        }catch (Exception exception){
            throw new IOException("Failed to remove note!",exception);
        }
    }

    /**
     * Release any underlying resources
     */
    @Override
    public void close() {

    }

    /**
     * Get NotebookRepo settings got the given user. Not supported.
     *
     * @param subject
     * @return
     */
    @Override
    public List<NotebookRepoSettingsInfo> getSettings(AuthenticationInfo subject) {
        return null;
    }

    /**
     * update notebook repo settings.
     *
     * @param settings
     * @param subject
     */
    @Override
    public void updateSettings(Map<String, String> settings, AuthenticationInfo subject) {
    }
    // Returns the root Directory. Most operations on notebooks and directories start from here.
    public Directory root(){
        return root;
    }

    // Creates a new empty Directory to another Directory
    // parentId = Directory id to add a new directory to
    // directoryName = full name of the Directory
    public void createDirectory(String parentId, String directoryName) throws Exception {
        try{
            ZeppelinFile parent = root.findFile(parentId);
            if(!parent.isDirectory()){
                throw new Exception("No such parent!");
            }
            Path directoryPath = Paths.get(parent.path().toString(),directoryName);
            String directoryId = extractIDFromFileName(directoryPath);
            Directory newDirectory = new Directory(directoryId,directoryPath,new ConcurrentHashMap<>());
            newDirectory.save();
            root = createRootDirectory(root.children());
        }catch (Exception exception){
            throw new Exception("Failed to create directory!",exception);
        }
    };

    // Debugging method for printing out the current tree to logger
    private void printTree(){
        for (Map.Entry<String, ZeppelinFile> entry: root.children().entrySet()) {
            LOGGER.debug("ID: {}, Path: {}",entry.getKey(),entry.getValue().path());
            entry.getValue().printTree();
        }
    }

    public Path rootDir(){
        return rootDir;
    }
}
