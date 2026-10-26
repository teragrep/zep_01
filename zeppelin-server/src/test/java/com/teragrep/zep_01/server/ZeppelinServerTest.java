package com.teragrep.zep_01.server;

import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.notebook.Notebook;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.utils.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

public final class ZeppelinServerTest {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final File zeppelinHome = new File("target/zeppelin");
    private final File confDir = new File(zeppelinHome, "/conf");
    private final File notebookDir = new File(zeppelinHome.getAbsolutePath() + "/notebook");
    private final File interpreterDir = new File(zeppelinHome, "/interpreter");

    private Future<Integer> startServer(){
        ZeppelinConfiguration.reset();
        // copy the resources files to a temp folder
        Assertions.assertDoesNotThrow(()->FileUtils.copyDirectory(new File("src/test/resources/notebook"),notebookDir));
        Assertions.assertDoesNotThrow(()->FileUtils.copyDirectory(new File("src/test/resources/conf"), confDir));
        Assertions.assertDoesNotThrow(()->FileUtils.copyDirectory(new File("src/test/resources/interpreter"), interpreterDir));

        System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME.getVarName(),
                zeppelinHome.getAbsolutePath());
        System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(),
                confDir.getAbsolutePath());
        System.setProperty(
                ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT.getVarName(),
                "spark");
        System.setProperty(
                ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(),
                notebookDir.getPath()
        );
        System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_WAR.getVarName(),
                new File("target/zeppelin-web/dist").getAbsolutePath());
        System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_ANGULAR_WAR.getVarName(),
                new File("target/zeppelin-web-angular/dist").getAbsolutePath());

        //set deprecated system property for note autorun to check that any legacy configuration values are properly ignored
        System.setProperty("zeppelin.notebook.run.id", "2A94M5J1Z");
        System.setProperty("zeppelin.notebook.run.autoshutdown", "true");

        final ZeppelinConfiguration conf = ZeppelinConfiguration.create();
        TestUtils.clearInstances();

        final Callable<Integer> server = new Callable<Integer>() {
            @Override
            public Integer call() {
                try {
                    ZeppelinServer.main(new String[]{""});
                    return 0;
                } catch (final Throwable e) {
                    return 1;
                }
            }
        };
        return executor.submit(server);
    }

    @Test
    public void testAutoRunShutdownDisabled() {

        // store time of test start, then start the server.
        final Instant testStartTime = Instant.now();
        final Future<Integer> serverTask = startServer();
        // wait until server is started
        final long timeout = Instant.now().toEpochMilli() + 10000;
        while (Instant.now().toEpochMilli() < timeout) {
            Assertions.assertDoesNotThrow(()->Thread.sleep(1000));
            final Notebook notebook = ZeppelinServer.sharedServiceLocator.getService(Notebook.class);
            if(notebook != null){
                break;
            }
        }
        // make sure no paragraph was started automatically when server started.
        final Notebook notebook = ZeppelinServer.sharedServiceLocator.getService(Notebook.class);
        final List<Paragraph> paragraphs  = Assertions.assertDoesNotThrow(()-> notebook.getNote("2A94M5J1Z").getParagraphs());
        int i = 0;
        while(i < paragraphs.size()){
            if(paragraphs.get(i).getDateStarted() != null){
                Assertions.assertTrue(paragraphs.get(i).getDateStarted().toInstant().isBefore(testStartTime));
            }
            i++;
        }
        Assertions.assertEquals(paragraphs.size(),i);
        // server should not shut down automatically.
        Assertions.assertThrows(TimeoutException.class,()->serverTask.get(2000, TimeUnit.MILLISECONDS));
    }
}
