package com.teragrep.zep_01;

import com.teragrep.zep_01.notebook.LegacyNotebook;
import com.teragrep.zep_01.notebook.Notebook;
import com.teragrep.zep_01.notebook.repo.Directory;
import com.teragrep.zep_01.notebook.repo.NotebookRepository;
import com.teragrep.zep_01.notebook.repo.ZeppelinFile;
import org.apache.commons.io.FileUtils;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.display.AngularObjectRegistryListener;
import com.teragrep.zep_01.interpreter.AbstractInterpreterTest;
import com.teragrep.zep_01.interpreter.InterpreterFactory;
import com.teragrep.zep_01.interpreter.InterpreterSettingManager;
import com.teragrep.zep_01.interpreter.remote.RemoteInterpreterProcessListener;
import com.teragrep.zep_01.notebook.Note;
import com.teragrep.zep_01.notebook.NoteInfo;
import com.teragrep.zep_01.notebook.utility.IdHashes;
import com.teragrep.zep_01.user.AuthenticationInfo;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;


public class NotebookRepositoryTest extends AbstractInterpreterTest{
    private final String zeppelinHome = "target";
    private final String notebookDir = "notebook";
    private final String testFile1Name = "my_note1_2A94M5J1Z.zpln";
    private final String testFile1Id = "2A94M5J1Z";
    private final String testFile1Origin =  "src/test/resources/testNotebooks/my_project/"+testFile1Name;
    private final String testFile2Name = "my_note2_2A94M5J2Z.zpln";
    private final String testFile2Id = "2A94M5J2Z";
    private final String testFile2Origin =  "src/test/resources/testNotebooks/my_project/"+testFile2Name;

    private final String testFile3Name = "my_note3_2A94M5J3Z.zpln";
    private final String testFile3Id = "2A94M5J3Z";
    private final String testFile3Origin =  "src/test/resources/testNotebooks/my_project/"+testFile3Name;

    private final String testFile4Name = "my_note4_2A94M5J4Z.zpln";
    private final String testFile4Id = "2A94M5J4Z";
    private final String testFile4Origin =  "src/test/resources/testNotebooks/my_project/"+testFile4Name;
    private final String testDirectory1Name = "myFolder";
    private final String testDirectory1Id = "1R7FGHJ";

    private final String testDirectory2Name = "myFolder";
    private final String testDirectory2Id = "2R7FGHJ";

    private final String testDirectory3Name = "myFolder";
    private final String testDirectory3Id = "3R7FGHJ";
    private final String delimiter = "_";
    private final String testNotebook1DisplayName = testFile1Name.substring(0,testFile1Name.lastIndexOf(delimiter));
    private final String testNotebook2DisplayName = testFile2Name.substring(0,testFile2Name.lastIndexOf(delimiter));
    private final String testNotebook3DisplayName = testFile3Name.substring(0,testFile3Name.lastIndexOf(delimiter));
    private final String testNotebook4DisplayName = testFile4Name.substring(0,testFile4Name.lastIndexOf(delimiter));
    private NotebookRepository repo;
    private ZeppelinConfiguration conf;
    private InterpreterSettingManager interpreterSettingManager;
    private InterpreterFactory interpreterFactory;
    private long startMillis;

    @FunctionalInterface
    public interface Executable {
        void execute() throws Exception;
    }
    // This method is a custom JUnit4 implementation of Assertions.assertDoesNotThrow, since JUnit4 does not have a similar built-in method.
    private void assertNoExceptionIsThrown(Executable executable) {
        try {
            executable.execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getClass().getSimpleName() + " was thrown");
        }
    }
    // Set up a notebook directory with two notebooks already saved in the legacy Zeppelin format, one of them inside a folder.
    @Override
    @Before
    public void setUp() {

        assertNoExceptionIsThrown(()->{
            File confInterpreterDir = new File("interpreter_" + getClass().getSimpleName());
            File interpreterDir = new File(zeppelinHome,"interpreter_" + getClass().getSimpleName());
            File confDir = new File(zeppelinHome, "conf_" + getClass().getSimpleName());

            interpreterDir.mkdirs();
            confDir.mkdirs();

            FileUtils.copyDirectory(new File("src/test/resources/interpreter"), interpreterDir);
            FileUtils.copyDirectory(new File("src/test/resources/conf"), confDir);


            if(Files.exists(Paths.get(zeppelinHome,notebookDir))){
                deleteDirectoryRecursively(Paths.get(zeppelinHome,notebookDir));
            }
            copyFile(Paths.get(testFile1Origin),Paths.get(zeppelinHome,notebookDir+"/"+testFile1Name));
            copyFile(Paths.get(testFile2Origin),Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testFile2Name));
            copyFile(Paths.get(testFile3Origin),Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testDirectory2Name+delimiter+testDirectory2Id+"/"+testFile3Name));
            copyFile(Paths.get(testFile4Origin),Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testFile4Name));
            Files.createDirectories(Paths.get(zeppelinHome,notebookDir+"/"+testDirectory3Name +delimiter+testDirectory3Id));

            conf = ZeppelinConfiguration.create();
            conf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME.getVarName(),zeppelinHome);
            conf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(),notebookDir);
            conf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_DIR.getVarName(),confInterpreterDir.toString());
            conf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(),confDir.getName());
            conf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT.getVarName(), "test");

            interpreterSettingManager = new InterpreterSettingManager(conf, null, null);
            interpreterFactory = new InterpreterFactory(interpreterSettingManager);

            repo = new NotebookRepository();
        });
        startMillis = System.currentTimeMillis();
    }

    // After ZeppelinServer calls repo.init() there should be representations of all saved files available.
    @Test
    public void testInit(){

        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            // All notes should exist
            Assert.assertEquals(testFile1Id,repo.root().findFile(testFile1Id).id());
            Assert.assertEquals(testFile2Id,repo.root().findFile(testFile2Id).id());
            Assert.assertEquals(testFile3Id,repo.root().findFile(testFile3Id).id());
            Assert.assertEquals(testFile4Id,repo.root().findFile(testFile4Id).id());

            // Directories should exist too
            Assert.assertEquals(testDirectory1Id,repo.root().findFile(testDirectory1Id).id());
            Assert.assertEquals(testDirectory2Id,repo.root().findFile(testDirectory2Id).id());

            // Directories should contain the right number of children
            Assert.assertEquals(3,repo.root().children().size());
            Assert.assertEquals(3,repo.root().findFile(testDirectory1Id).children().size());
            Assert.assertEquals(1,repo.root().findFile(testDirectory2Id).children().size());
        });
    }
    @Test
    public void testLoad() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile file1 = repo.root().findFile(testFile1Id).load();
            ZeppelinFile file2 = repo.root().findFile(testFile2Id).load();
            ZeppelinFile file3 = repo.root().findFile(testFile3Id).load();
            ZeppelinFile file4 = repo.root().findFile(testFile4Id).load();

            Assert.assertEquals(file1.id(),testFile1Id);
            Assert.assertEquals(file2.id(),testFile2Id);
            Assert.assertEquals(file3.id(),testFile3Id);
            Assert.assertEquals(file4.id(),testFile4Id);
            
            Assert.assertFalse(file1.isDirectory());
            Assert.assertFalse(file2.isDirectory());
            Assert.assertFalse(file3.isDirectory());
            Assert.assertFalse(file4.isDirectory());
        });
    }
    @Test
    public void testList() {

        assertNoExceptionIsThrown(() -> {
            repo.init(conf);
            Map<String, NoteInfo> allNotebooks = repo.list(AuthenticationInfo.ANONYMOUS);
            Assert.assertEquals(allNotebooks.size(), 4);
            Assert.assertEquals(testNotebook1DisplayName,allNotebooks.get(testFile1Id).getNoteName());
            Assert.assertEquals(testNotebook2DisplayName,allNotebooks.get(testFile2Id).getNoteName());
            Assert.assertEquals(testNotebook3DisplayName,allNotebooks.get(testFile3Id).getNoteName());
            Assert.assertEquals(testNotebook4DisplayName,allNotebooks.get(testFile4Id).getNoteName());

            Assert.assertEquals(testFile1Name,allNotebooks.get(testFile1Id).getPath()+"_"+testFile1Id+".zpln");
            Assert.assertEquals(testDirectory1Name+delimiter+testDirectory1Id+"/"+testFile2Name,allNotebooks.get(testFile2Id).getPath()+"_"+testFile2Id+".zpln");
            Assert.assertEquals(testDirectory1Name+delimiter+testDirectory1Id+"/"+testDirectory2Name+delimiter+testDirectory2Id+"/"+testFile3Name,allNotebooks.get(testFile3Id).getPath()+"_"+testFile3Id+".zpln");
            Assert.assertEquals(testDirectory1Name+delimiter+testDirectory1Id+"/"+testFile4Name,allNotebooks.get(testFile4Id).getPath()+"_"+testFile4Id+".zpln");
            });
    }
    @Test
    public void testGetNote() {

        assertNoExceptionIsThrown(()->{
        repo.init(conf);
        Note note = repo.get(testFile1Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertEquals(note.getId(),testFile1Id);});
    }
    @Test
    public void testGetNoteFromDirectory() {

        assertNoExceptionIsThrown(()->{
        repo.init(conf);
        Note note = repo.get(testFile2Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertEquals(note.getId(),testFile2Id);

        Note noteWithinSubDirectory = repo.get(testFile3Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertEquals(noteWithinSubDirectory.getId(),testFile3Id);});
    }
    @Test
    public void testSaveNewNotebook() {

        assertNoExceptionIsThrown(()->{
        repo.init(conf);
        String id = IdHashes.generateId();
        Path pathToSave = Paths.get(repo.rootDir()+"/testSave_"+id+".zpln");
        com.teragrep.zep_01.notebook.Notebook newNotebook = new com.teragrep.zep_01.notebook.Notebook("testTitle",id,pathToSave,new LinkedHashMap<>());
        Assert.assertFalse(Files.exists(pathToSave));
        newNotebook.save();
        //repo.save(id,AuthenticationInfo.ANONYMOUS);
        Assert.assertTrue(Files.exists(pathToSave));});
    }
    @Test
    public void testMoveNotebook() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile notebooktoMove = repo.root().findFile(testFile1Id).load();
            Path oldPath = notebooktoMove.path();

            // Create a new directory to which we will move our notebook.
            String newDirectoryId = "123TESTID";
            String newDirectoryName = "newDirectory";
            Path directoryPath = Paths.get(repo.rootDir() + "/"+newDirectoryName+"_"+newDirectoryId);
            Directory newDirectory = new Directory(newDirectoryId, directoryPath);
            newDirectory.save();

            // Move the Notebook to the new directory.
            notebooktoMove.move(Paths.get(String.valueOf(directoryPath),notebooktoMove.path().getFileName().toString()));

            // After calling move(), old file should be removed from disk, and a new file should be saved in proper path.
            Assert.assertTrue(Files.exists(Paths.get(newDirectory.path().toString(),notebooktoMove.path().getFileName().toString())));
            Assert.assertFalse(Files.exists(oldPath));

            // After Re-initializing the notebookRepository, the moved notebook should be under the correct Directory.
            repo.init(conf);
            Assert.assertTrue(repo.root().findFile(newDirectoryId).children().containsKey(testFile1Id));
            Assert.assertFalse(repo.root().children().containsKey(testFile1Id));
        });
    }

    @Test
    public void testRenameNotebook() {
        assertNoExceptionIsThrown(()->{
            //repo.init(conf);

            // Original file should exist, new name should not.
            Path originalPath = Paths.get(repo.root().path().toString(),"my_note1_"+testFile1Id+".zpln");
            Path newPath = Paths.get(repo.root().path().toString(),"newNameNewMe_"+testFile1Id+".zpln");
            Assert.assertTrue(Files.exists(originalPath));
            Assert.assertFalse(Files.exists(newPath));
            ZeppelinFile notebook = repo.root().findFile(testFile1Id).load();
            notebook.rename("newNameNewMe_"+testFile1Id+".zpln");

            // Original file should not exist anymore and new name should.
            Assert.assertFalse(Files.exists(originalPath));
            Assert.assertTrue(Files.exists(newPath));

            // Renaming should be reflected in root's Directory structure after re-initalizing NotebookRepo
            repo.init(conf);
            ZeppelinFile renamedNotebook = repo.root().findFile(testFile1Id);
            Assert.assertEquals(newPath,renamedNotebook.path());
        });}
    @Test
    public void testRenameNotebookInDirectory() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);

            Path originalPath = Paths.get(repo.root().path().toString(),testDirectory1Name+"_"+testDirectory1Id,testDirectory2Name+"_"+testDirectory2Id,"my_note3_"+testFile3Id+".zpln");
            Path newPath = Paths.get(repo.root().path().toString(),testDirectory1Name+"_"+testDirectory1Id,testDirectory2Name+"_"+testDirectory2Id,"newNameNewMe_"+testFile3Id+".zpln");

            // Original file should exist, new file should not.
            Assert.assertTrue(Files.exists(originalPath));
            Assert.assertFalse(Files.exists(newPath));

            ZeppelinFile notebook = repo.root().findFile(testFile3Id).load();
            notebook.rename("newNameNewMe_"+testFile3Id+".zpln");

            // Original file should not exist anymore, new file should
            Assert.assertFalse(Files.exists(originalPath));
            Assert.assertTrue(Files.exists(newPath));

            // Renaming should be reflected in root's Directory structure after re-initalizing NotebookRepo
            repo.init(conf);
            ZeppelinFile renamedNotebook = repo.root().findFile(testFile3Id);
            Assert.assertEquals(newPath,renamedNotebook.path());
        });
    }

    @Test
    public void testMoveDirectory() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile originalDirectory = repo.root().findFile(testDirectory2Id);
            Path moveTo = Paths.get(repo.rootDir() + "/newFolder");
            Path originalPath = originalDirectory.path();


            Assert.assertFalse(Files.exists(moveTo));
            Assert.assertTrue(Files.exists(originalPath));

            originalDirectory.move(moveTo);

            Assert.assertTrue(Files.exists(moveTo));
            Assert.assertFalse(Files.exists(originalPath));

            // Moving should be reflected in root's Directory structure after re-initalizing NotebookRepo
            // Original parent (testDirectory1) should no longer contain the moved directory after reinitialization.
            Assert.assertTrue(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            repo.init(conf);
            Assert.assertFalse(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            Assert.assertFalse(repo.root().children().containsKey(testDirectory2Id));
        });
    }

    // You should be able to move a directory into another directory.
    @Test
    public void testMoveDirectoryUnderAnotherDirectory() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile originalDirectory = repo.root().findFile(testDirectory2Id);
            ZeppelinFile destinationDirectory = repo.root().findFile(testDirectory3Id);
            Path originalPath = originalDirectory.path();


            Assert.assertTrue(Files.exists(originalPath));
            originalDirectory.move(Paths.get(destinationDirectory.path().toString(),testDirectory2Name+delimiter+testDirectory2Id));
            Assert.assertFalse(Files.exists(originalPath));

            // Moving should be reflected in root's Directory structure after re-initalizing NotebookRepo
            // Original parent (testDirectory1) should no longer contain the moved directory after reinitialization.
            Assert.assertTrue(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            repo.init(conf);
            Assert.assertFalse(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            Assert.assertTrue(repo.root().findFile(testDirectory3Id).children().containsKey(testDirectory2Id));
        });
    }

    // You should not be able to move a parent Directory under its own child Directory.
    // Doing that would break the tree structure and orphan the moved files
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();
    @Test
    public void testMoveParentDirectoryUnderChildDirectory() throws IOException {
        repo.init(conf);
        ZeppelinFile originalDirectory = repo.root().findFile(testDirectory1Id);
        ZeppelinFile destinationDirectory = repo.root().findFile(testDirectory2Id);
        Path originalPath = originalDirectory.path();

        Assert.assertTrue(Files.exists(originalPath));

        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Cannot move a directory into one of its own children!");
        // This should throw an IOException with the above message.
        originalDirectory.move(Paths.get(destinationDirectory.path().toString(),testDirectory1Name+delimiter+testDirectory1Id));;
    }

    @Test
    public void testRenameDirectory() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile directory = repo.root().findFile(testDirectory1Id);
            String renameTo = "renamedDirectory"+delimiter+ testDirectory1Id;
            Assert.assertTrue(Files.exists(Paths.get(repo.root().path().toString(), testDirectory1Name +delimiter+ testDirectory1Id)));
            Assert.assertFalse(Files.exists(Paths.get(repo.root().path().toString(), renameTo)));

            directory.rename(renameTo);

            Assert.assertFalse(Files.exists(Paths.get(repo.root().path().toString(), testDirectory1Name +delimiter+ testDirectory1Id)));
            Assert.assertTrue(Files.exists(Paths.get(repo.root().path().toString(), renameTo)));

            // Moving should be reflected in root's Directory structure after re-initalizing NotebookRepo
            Assert.assertTrue(repo.root().children().containsKey(testDirectory1Id));
            repo.init(conf);
            // Position in Directory structure should remain unchanged.
            Assert.assertTrue(repo.root().children().containsKey(testDirectory1Id));
            Assert.assertEquals(repo.root().findFile(testDirectory1Id).path(),Paths.get(repo.root().path().toString(),renameTo));
        });
    }
    
    @Test
    public void testMoveDirectoryToAnotherDirectory() {
        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            ZeppelinFile originalDirectory = repo.root().findFile(testDirectory2Id);
            Path moveTo = Paths.get(repo.rootDir() + "/newFolder");
            Path originalPath = originalDirectory.path();


            Assert.assertFalse(Files.exists(moveTo));
            Assert.assertTrue(Files.exists(originalPath));

            originalDirectory.move(moveTo);

            Assert.assertTrue(Files.exists(moveTo));
            Assert.assertFalse(Files.exists(originalPath));

            // Moving should be reflected in root's Directory structure after re-initalizing NotebookRepo
            // Original parent (testDirectory1) should no longer contain the moved directory after reinitialization.
            Assert.assertTrue(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            repo.init(conf);
            Assert.assertFalse(repo.root().findFile(testDirectory1Id).children().containsKey(testDirectory2Id));
            Assert.assertFalse(repo.root().children().containsKey(testDirectory2Id));
        });
    }

    @Test
    public void testRemoveNotebook() {
        assertNoExceptionIsThrown(()->{
        repo.init(conf);

        // Remove every file in sequence and verify that the files no longer exist after deletion.
        Assert.assertTrue(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+testFile1Name)));
        repo.remove(testFile1Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertFalse(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+testFile1Name)));

        Assert.assertTrue(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testFile2Name)));
        repo.remove(testFile2Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertFalse(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testFile2Name)));

        Assert.assertTrue(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testDirectory2Name+delimiter+testDirectory2Id+"/"+testFile3Name)));
        repo.remove(testFile3Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertFalse(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testDirectory2Name+delimiter+testDirectory2Id+"/"+testFile3Name)));

        Assert.assertTrue(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +delimiter+ testDirectory1Id +"/"+testFile4Name)));
        repo.remove(testFile4Id,"",AuthenticationInfo.ANONYMOUS);
        Assert.assertFalse(Files.exists(Paths.get(zeppelinHome,notebookDir+"/"+ testDirectory1Name +"/"+testFile4Name)));

        // Root directory should only have two directories left after all notebooks are deleted.
        Assert.assertEquals(2,repo.root().children().size());});
    }
    @Test
    public void testRemoveDirectory() {

        assertNoExceptionIsThrown(()-> {
            repo.init(conf);
            Assert.assertTrue(Files.exists(Paths.get(String.valueOf(repo.rootDir()), testDirectory1Name +delimiter+ testDirectory1Id)));
            repo.remove(testDirectory1Id, AuthenticationInfo.ANONYMOUS);
            Assert.assertFalse(Files.exists(Paths.get(String.valueOf(repo.rootDir()), testDirectory1Name +delimiter+ testDirectory1Id)));
        });
    }

    @Test
    public void testCopyNotebook() {

        assertNoExceptionIsThrown(()-> {
            repo.init(conf);

            // Generate an id for the copy and ensure a file with that ID does not exist yet
            String copiedNotebookId = IdHashes.generateId();
            Assert.assertFalse(Files.exists(Paths.get(zeppelinHome,notebookDir, "copiedNotebook_" + copiedNotebookId)));
            Path copyPath = Paths.get(zeppelinHome,notebookDir, "copiedNotebook_" + copiedNotebookId+".zpln");

            // Find the source notebook and call its' copy method and save the copied object
            ZeppelinFile notebook = repo.root().findFile(testFile1Id);
            Path originalPath = notebook.path();

            Notebook loadedNotebook = (Notebook) notebook.load();
            Notebook copiedNotebook = loadedNotebook.copy(copyPath, copiedNotebookId);

            // Assert that both the copied notebook's and the original notebook's files exist
            Assert.assertTrue(Files.exists(copyPath));
            Assert.assertTrue(Files.exists(originalPath));

            // Check that copied notebook has proper ID assigned, and same ID is in the file
            Assert.assertEquals(copiedNotebookId, copiedNotebook.id());
            Assert.assertTrue(copiedNotebook.json().toString().contains("\"id\":\"" + copiedNotebookId + "\""));
        });
    }

    @Test
    public void testCopyDirectory() {

        assertNoExceptionIsThrown(()->{
            repo.init(conf);
            // Generate a random ID for the copied directory.
            String copiedDirectoryId = IdHashes.generateId();

            // Get reference to the original directory
            ZeppelinFile directory = repo.root().findFile(testDirectory1Id);
            Path originalPath = directory.path();
            Path copyPath = Paths.get(zeppelinHome,notebookDir,"copiedDirectory_"+copiedDirectoryId);

            // Copied directory should not exist yet, original directory should exist.
            Assert.assertTrue(Files.exists(originalPath));
            Assert.assertFalse(Files.exists(copyPath));

            directory.copy(copyPath,copiedDirectoryId);

            // Copied directory should exist now, original directory should also exist.
            Assert.assertTrue(Files.exists(originalPath));
            Assert.assertTrue(Files.exists(copyPath));

            // Copied directory should be reflected in root's directory tree after re-initializing NotebookRepository.
            Assert.assertEquals(7,repo.root().listAllChildren().size());
            repo.init(conf);
            ZeppelinFile copiedDirectory = repo.root().findFile(copiedDirectoryId);
            ZeppelinFile originalDirectory = repo.root().findFile(testDirectory1Id);
            // Root should have one more child (the directory we copied)
            Assert.assertEquals(12,repo.root().listAllChildren().size());
            // Copied directory has to contain the same amount of children as its origin.
            Assert.assertEquals(copiedDirectory.children().size(),originalDirectory.children().size());
            // None of the children that were copied should have the same ID as any of the children in the original directory.
            for (ZeppelinFile copiedChild : copiedDirectory.listAllChildren()) {
                Assert.assertFalse(originalDirectory.children().containsKey(copiedChild.id()));
            }
        });
    }

    @Test
    public void createNotebookTest() {

        assertNoExceptionIsThrown(()-> {
            repo.init(conf);
        });
    }

    private  void deleteDirectoryRecursively(Path directoryToDelete) {
        assertNoExceptionIsThrown(()->{
        Stream<Path> walk = Files.walk(directoryToDelete);
            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        });
    }

    private void copyFile(Path from, Path to){
        assertNoExceptionIsThrown(()->{
        if(!Files.exists(to)) {
            Files.createDirectories(to.getParent());
            Files.copy(from, to);
        }
    });
    }
    @Override
    @After
    public void tearDown(){
        System.out.println("Took ms: "+ (System.currentTimeMillis() - startMillis));
        if(Files.exists(Paths.get(zeppelinHome,notebookDir))){
            deleteDirectoryRecursively(Paths.get(zeppelinHome,notebookDir));
        }
    }
}
