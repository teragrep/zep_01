package com.teragrep.zep_01;

import com.teragrep.zep_01.notebook.Notebook;
import com.teragrep.zep_01.notebook.Paragraph;
import com.teragrep.zep_01.notebook.Result;
import com.teragrep.zep_01.notebook.Script;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import com.teragrep.zep_01.interpreter.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class NotebookTest {

    private final String notebookName = "testNotebook";
    private final String notebookId = "2A94M5J1Z";
    private final Path newFormatNotebookPath = Paths.get("src/test/resources/testNotebooks/my_project/my_new_note1_2A94M5J1Z.zpln");
    private final Path legacyFormatNotebookPath = Paths.get("src/test/resources/testNotebooks/my_project/my_note2_2A94M5J2Z.zpln");
    private final Path targetNotebookDir = Paths.get("target/notebook/");

    private final String paragraphId1 = "paragraph_34013249102";
    private final String paragraphId2 = "paragraph_65324234523";
    private final String errorParagraphId = "paragraph_4362340312";
    private final String paragraphName1 = "Paragraph 1 title";
    private final String paragraphName2 = "Paragraph 2 title";
    private final String errorParagraphName = "This paragraph will cause an error";

    private final String scriptText1 = "%md Hello world!";
    private final String scriptText2 = "%md Hello another paragraph!";
    private final String errorScriptText = "goofy script that causes an error";
    private final String user = "anonymous";
    private final InterpreterResult emptyInterpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS,new ArrayList<InterpreterResultMessage>());
    private final Properties interpreterProperties = new Properties();
    private final EchoInterpreter interpreter = new EchoInterpreter(interpreterProperties);
    private final Properties errorInterpreterProperties = new Properties();
    private final EchoInterpreter errorInterpreter = new EchoInterpreter(new Properties());
    private final InterpreterSetting setting = new InterpreterSetting.Builder()
            .setName("echo")
            .setGroup("test")
            .setProperties(interpreterProperties)
            .setInterpreterInfos(new ArrayList<>())
            .create();
    private final InterpreterSetting errorInterpreterSetting = new InterpreterSetting.Builder()
            .setName("echo")
            .setGroup("test")
            .setProperties(errorInterpreterProperties)
            .setInterpreterInfos(new ArrayList<>())
            .create();
    private final ExecutionContext context = new ExecutionContext(user,notebookId,"echo");
    private final ExecutionContext errorContext = new ExecutionContext(user,notebookId,"echo");
    private final InterpreterContext intpcontext = InterpreterContext.builder().build();
    private final ManagedInterpreterGroup interpreterGroup = setting.getOrCreateInterpreterGroup(context);
    private final ManagedInterpreterGroup errorInterpreterGroup = errorInterpreterSetting.getOrCreateInterpreterGroup(errorContext);
    private final Notebook notebook;

    public NotebookTest() {
        interpreter.setInterpreterGroup(interpreterGroup);
        interpreter.open();

        errorInterpreter.setProperty("zeppelin.interpreter.echo.fail","true");
        errorInterpreter.setInterpreterGroup(errorInterpreterGroup);
        errorInterpreter.open();

        LinkedHashMap<String, Paragraph> paragraphs = new LinkedHashMap<>();
        Result emptyResult = new Result(emptyInterpreterResult.code(),emptyInterpreterResult.message());

        // Create paragraph
        Script script1 = new Script(scriptText1, interpreter,intpcontext);
        Paragraph paragraph1 = new Paragraph(paragraphId1, paragraphName1,emptyResult,script1);

        // Create another paragraph
        Script script2 = new Script(scriptText2, interpreter,intpcontext);
        Paragraph paragraph2 = new Paragraph(paragraphId2, paragraphName2,emptyResult,script2);

        // Create paragraph that contains user code with an error
        Script errorScript = new Script(errorScriptText, errorInterpreter,intpcontext);
        Paragraph errorParagraph = new Paragraph(errorParagraphId, errorParagraphName,emptyResult,errorScript);

        paragraphs.put(paragraphId1,paragraph1);
        paragraphs.put(paragraphId2,paragraph2);
        paragraphs.put(errorParagraphId,errorParagraph);
        notebook = new Notebook(notebookName,notebookId,Paths.get(targetNotebookDir.toString(),notebookName+"_"+notebookId+".zpln"),paragraphs);
    }

    @After
    public void cleanup() throws IOException {
        if(Files.exists(targetNotebookDir)){
            for (Path path: Files.list(targetNotebookDir).collect(Collectors.toList())) {
                path.toFile().delete();
            }
        }
    }

    // This method is a custom JUnit4 implementation of Assertions.assertDoesNotThrow, since JUnit4 does not have a similar built-in method.
    private void assertNoExceptionIsThrown(NotebookRepositoryTest.Executable executable) {
        try {
            executable.execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getClass().getSimpleName() + " was thrown");
        }
    }
    @Test
    public void runSingleParagraphTest(){
        assertNoExceptionIsThrown(()->{
            Map<String,Paragraph> paragraphs = notebook.paragraphs();
            Result result = paragraphs.get(paragraphId1).run();
            Assert.assertTrue(result.code().equals(InterpreterResult.Code.SUCCESS));
            Assert.assertTrue(result.messages().size() == 1);
            Assert.assertEquals("%text Hello world!",result.messages().get(0).toString());
        });
    }
    @Test
    public void runAllParagraphsTest() {
        assertNoExceptionIsThrown(()->{
            Notebook executedNotebook = notebook.runAll();

            Assert.assertTrue(executedNotebook.paragraphs().get(paragraphId1).result().code().equals(InterpreterResult.Code.SUCCESS));
            Assert.assertTrue(executedNotebook.paragraphs().get(paragraphId1).result().messages().size() == 1);
            assertEquals("Hello world!",executedNotebook.paragraphs().get(paragraphId1).result().messages().get(0).getData());

            Assert.assertTrue(executedNotebook.paragraphs().get(paragraphId2).result().code().equals(InterpreterResult.Code.SUCCESS));
            Assert.assertTrue(executedNotebook.paragraphs().get(paragraphId2).result().messages().size() == 1);
            assertEquals("Hello another paragraph!",executedNotebook.paragraphs().get(paragraphId2).result().messages().get(0).getData());

            Assert.assertTrue(executedNotebook.paragraphs().get(errorParagraphId).result().code().equals(InterpreterResult.Code.ERROR));
            Assert.assertTrue(executedNotebook.paragraphs().get(errorParagraphId).result().messages().size() == 0);
        });
    }

    @Test
    public void testSaveNotebook() {
        assertNoExceptionIsThrown(()->{
            // File should not exist before saving
            Assert.assertFalse(Files.exists(notebook.path()));
            Notebook executedNotebook = notebook.runAll();
            executedNotebook.save();
            // Executednotebook should have identical path to original notebook
            Assert.assertEquals(notebook.path(),executedNotebook.path());
            // File should exist after saving
            Assert.assertTrue(Files.exists(notebook.path()));
            BufferedReader reader = new BufferedReader(new FileReader(notebook.path().toFile()));
            String fileContents = reader.readLine();
            reader.close();
            // File contents should be as expected
            Assert.assertEquals(
                    "{\"id\":\"2A94M5J1Z\",\"name\":\"testNotebook\",\"config\":{},\"paragraphs\":[{\"id\":\"paragraph_34013249102\",\"title\":\"Paragraph 1 title\",\"script\":{\"text\":\"%md Hello world!\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"SUCCESS\",\"msg\":[{\"type\":\"TEXT\",\"data\":\"Hello world!\"}]},\"settings\":{},\"text\":\"%md Hello world!\",\"config\":{}},{\"id\":\"paragraph_65324234523\",\"title\":\"Paragraph 2 title\",\"script\":{\"text\":\"%md Hello another paragraph!\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"SUCCESS\",\"msg\":[{\"type\":\"TEXT\",\"data\":\"Hello another paragraph!\"}]},\"settings\":{},\"text\":\"%md Hello another paragraph!\",\"config\":{}},{\"id\":\"paragraph_4362340312\",\"title\":\"This paragraph will cause an error\",\"script\":{\"text\":\"goofy script that causes an error\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{\"zeppelin.interpreter.echo.fail\":\"true\"},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"ERROR\",\"msg\":[]},\"settings\":{},\"text\":\"goofy script that causes an error\",\"config\":{}}]}"
                    ,fileContents);
        });
    }

    // Test initializing a notebook from an old Zeppelin example file
    @Test
    public void testInitFromLegacyFile() {
        assertNoExceptionIsThrown(()->{
                JsonReader jsonReader = Json.createReader(new FileReader(legacyFormatNotebookPath.toFile()));
                JsonObject json = jsonReader.readObject();
                jsonReader.close();
                List<JsonObject> paragraphJson = json.getJsonArray("paragraphs").stream().map(JsonValue::asJsonObject).collect(Collectors.toList());
                LinkedHashMap<String,Paragraph> paragraphs = new LinkedHashMap<>();
                for (JsonObject jsonObject:paragraphJson) {
                    final List<InterpreterResultMessage> resultMessages;
                    final InterpreterResult.Code code;
                    if(jsonObject.containsKey("results")){
                        resultMessages = jsonObject.getJsonObject("results").getJsonArray("msg").getValuesAs(jsonValue ->{
                            String type = jsonValue.asJsonObject().getString("type");
                            String data = jsonValue.asJsonObject().getString("data");
                            return new InterpreterResultMessage(InterpreterResult.Type.valueOf(type),data);
                        } );
                        code = InterpreterResult.Code.valueOf(jsonObject.getJsonObject("results").getString("code"));
                    }
                    else {
                        resultMessages = new ArrayList<>();
                        resultMessages.add(new InterpreterResultMessage(InterpreterResult.Type.TEXT,""));
                        code = InterpreterResult.Code.SUCCESS;
                    }
                    Result result = new Result(code,resultMessages);

                    final String text;
                    if(jsonObject.containsKey("text")){
                        text = jsonObject.getString("text");
                    }
                    else {
                        text = "";
                    }
                    Script script = new Script(text,interpreter, InterpreterContext.builder().build());

                    final String title;
                    if(jsonObject.containsKey("title")){
                        title = jsonObject.getString("title");
                    }
                    else {
                        title = "";
                    }
                    paragraphs.put(jsonObject.getString("id"),new Paragraph(jsonObject.getString("id"),title,result,script));
                }
                Notebook notebook = new Notebook(json.getString("name"),json.getString("id"),Paths.get(""),paragraphs);
                List<Paragraph> paragraphList = notebook.paragraphs().values().stream().collect(Collectors.toList());

                // Assert that everything matches with what's saved on the file.
                assertEquals("my_note2",notebook.title());
                assertEquals("2A94M5J2Z",notebook.id());
                assertEquals(3,notebook.paragraphs().size());
                assertEquals(paragraphList.get(0).id(),"20150213-230428_1231780373");
                assertEquals(paragraphList.get(1).id(),"20150326-214658_12335843");
                assertEquals(paragraphList.get(2).id(),"20150703-133047_853701097");
                assertEquals(paragraphList.get(0).title(),"");
                assertEquals(paragraphList.get(1).title(),"");
                assertEquals(paragraphList.get(2).title(),"");
                assertEquals(paragraphList.get(0).script().text(),"%test\n## Congratulations, it's done.\n##### You can create your own notebook in 'Notebook' menu. Good luck!");
                assertEquals(paragraphList.get(1).script().text(),"%test\n\nAbout bank data\n\n```\nCitation Request:\n  This dataset is public available for research. The details are described in [Moro et al., 2011]. \n  Please include this citation if you plan to use this database:\n\n  [Moro et al., 2011] S. Moro, R. Laureano and P. Cortez. Using Data Mining for Bank Direct Marketing: An Application of the CRISP-DM Methodology. \n  In P. Novais et al. (Eds.), Proceedings of the European Simulation and Modelling Conference - ESM'2011, pp. 117-121, Guimarães, Portugal, October, 2011. EUROSIS.\n\n  Available at: [pdf] http://hdl.handle.net/1822/14838\n                [bib] http://www3.dsi.uminho.pt/pcortez/bib/2011-esm-1.txt\n```");
                assertEquals(paragraphList.get(2).script().text(),"");
                assertEquals(paragraphList.get(0).result().messages().size(),1);
                assertEquals(paragraphList.get(1).result().messages().size(),1);
                assertEquals(paragraphList.get(2).result().messages().size(),1);
                assertEquals(paragraphList.get(0).result().messages().get(0).getData(),"<h2>Congratulations, it's done.</h2>\n" +
                        "<h5>You can create your own notebook in 'Notebook' menu. Good luck!</h5>\n");
                assertEquals(paragraphList.get(1).result().messages().get(0).getData(),"<p>About bank data</p>\n" +
                        "<pre><code>Citation Request:\n" +
                        "  This dataset is public available for research. The details are described in [Moro et al., 2011]. \n" +
                        "  Please include this citation if you plan to use this database:\n" +
                        "\n" +
                        "  [Moro et al., 2011] S. Moro, R. Laureano and P. Cortez. Using Data Mining for Bank Direct Marketing: An Application of the CRISP-DM Methodology. \n" +
                        "  In P. Novais et al. (Eds.), Proceedings of the European Simulation and Modelling Conference - ESM'2011, pp. 117-121, Guimarães, Portugal, October, 2011. EUROSIS.\n" +
                        "\n" +
                        "  Available at: [pdf] http://hdl.handle.net/1822/14838\n" +
                        "                [bib] http://www3.dsi.uminho.pt/pcortez/bib/2011-esm-1.txt\n" +
                        "</code></pre>\n");
                assertEquals(paragraphList.get(2).result().messages().get(0).getData(),"");
                assertEquals(paragraphList.get(0).result().messages().get(0).getType(), InterpreterResult.Type.HTML);
                assertEquals(paragraphList.get(1).result().messages().get(0).getType(), InterpreterResult.Type.HTML);
                assertEquals(paragraphList.get(2).result().messages().get(0).getType(), InterpreterResult.Type.TEXT);
        });
    }

    // Make sure you can create a Notebook object from a json file. File in question is at {newFormatNotebookPath}
    @Test
    public void testCreationFromNewFile() {
        assertNoExceptionIsThrown(()->{
            JsonReader jsonReader = Json.createReader(new FileReader(newFormatNotebookPath.toFile()));
            JsonObject json = jsonReader.readObject();
            jsonReader.close();
            List<JsonObject> paragraphJson = json.getJsonArray("paragraphs").stream().map(JsonValue::asJsonObject).collect(Collectors.toList());
            LinkedHashMap<String,Paragraph> paragraphs = new LinkedHashMap<>();
            for (JsonObject jsonObject:paragraphJson) {
                final List<InterpreterResultMessage> resultMessages;
                final InterpreterResult.Code code;
                if(jsonObject.containsKey("results")){
                    resultMessages = jsonObject.getJsonObject("results").getJsonArray("msg").getValuesAs(jsonValue ->{
                        String type = jsonValue.asJsonObject().getString("type");
                        String data = jsonValue.asJsonObject().getString("data");
                        return new InterpreterResultMessage(InterpreterResult.Type.valueOf(type),data);
                    } );
                    code = InterpreterResult.Code.valueOf(jsonObject.getJsonObject("results").getString("code"));
                }
                else {
                    resultMessages = new ArrayList<>();
                    resultMessages.add(new InterpreterResultMessage(InterpreterResult.Type.TEXT,""));
                    code = InterpreterResult.Code.SUCCESS;
                }
                Result result = new Result(code,resultMessages);

                final String text;
                if(jsonObject.containsKey("script")){
                    JsonObject scriptObject = jsonObject.getJsonObject("script");
                    if(scriptObject.containsKey("text")){
                        text = scriptObject.getString("text");
                    }
                    else {
                        text = "";
                    }
                }
                else {
                    text = "";
                }
                Script script = new Script(text,interpreter, InterpreterContext.builder().build());

                final String title;
                if(jsonObject.containsKey("title")){
                    title = jsonObject.getString("title");
                }
                else {
                    title = "";
                }
                paragraphs.put(jsonObject.getString("id"),new Paragraph(jsonObject.getString("id"),title,result,script));
            }
            Notebook notebook = new Notebook(json.getString("name"),json.getString("id"),Paths.get(""),paragraphs);

            Map<String,Paragraph> paragraphMap = notebook.paragraphs();
            List<Paragraph> paragraphList = paragraphMap.values().stream().collect(Collectors.toList());

            // Assert that everything matches with what's saved on the file.
            assertEquals("my_note2",notebook.title());
            assertEquals("2A94M5J2Z",notebook.id());
            assertEquals(2,notebook.paragraphs().size());
            assertEquals(paragraphList.get(0).id(),"20150213-230428_1231780373");
            assertEquals(paragraphList.get(1).id(),"20150326-214658_12335843");
            assertEquals(paragraphList.get(0).title(),"");
            assertEquals(paragraphList.get(1).title(),"");
            assertEquals(paragraphList.get(0).script().text(),"%md\n## Congratulations, it's done.\n##### You can create your own notebook in 'Notebook' menu. Good luck!");
            assertEquals(paragraphList.get(1).script().text(),"%md\n\nAbout bank data\n\n```\nCitation Request:\n  This dataset is public available for research. The details are described in [Moro et al., 2011]. \n  Please include this citation if you plan to use this database:\n\n  [Moro et al., 2011] S. Moro, R. Laureano and P. Cortez. Using Data Mining for Bank Direct Marketing: An Application of the CRISP-DM Methodology. \n  In P. Novais et al. (Eds.), Proceedings of the European Simulation and Modelling Conference - ESM'2011, pp. 117-121, Guimarães, Portugal, October, 2011. EUROSIS.\n\n  Available at: [pdf] http://hdl.handle.net/1822/14838\n                [bib] http://www3.dsi.uminho.pt/pcortez/bib/2011-esm-1.txt\n```");
            assertEquals(paragraphList.get(0).result().messages().size(),1);
            assertEquals(paragraphList.get(1).result().messages().size(),1);
            assertEquals(paragraphList.get(0).result().messages().get(0).getData(),"<h2>Congratulations, it's done.</h2>\n" +
                    "<h5>You can create your own notebook in 'Notebook' menu. Good luck!</h5>\n");
            assertEquals(paragraphList.get(1).result().messages().get(0).getData(),"<p>About bank data</p>\n" +
                    "<pre><code>Citation Request:\n" +
                    "  This dataset is public available for research. The details are described in [Moro et al., 2011]. \n" +
                    "  Please include this citation if you plan to use this database:\n" +
                    "\n" +
                    "  [Moro et al., 2011] S. Moro, R. Laureano and P. Cortez. Using Data Mining for Bank Direct Marketing: An Application of the CRISP-DM Methodology. \n" +
                    "  In P. Novais et al. (Eds.), Proceedings of the European Simulation and Modelling Conference - ESM'2011, pp. 117-121, Guimarães, Portugal, October, 2011. EUROSIS.\n" +
                    "\n" +
                    "  Available at: [pdf] http://hdl.handle.net/1822/14838\n" +
                    "                [bib] http://www3.dsi.uminho.pt/pcortez/bib/2011-esm-1.txt\n" +
                    "</code></pre>\n");
            assertEquals(paragraphList.get(0).result().messages().get(0).getType(), InterpreterResult.Type.HTML);
            assertEquals(paragraphList.get(1).result().messages().get(0).getType(), InterpreterResult.Type.HTML);
        });

    }
    @Test
    public void testJson() {
        assertNoExceptionIsThrown(()->{
            JsonObject json = notebook.json();
            Assert.assertEquals("{\"id\":\"2A94M5J1Z\",\"name\":\"testNotebook\",\"config\":{},\"paragraphs\":[{\"id\":\"paragraph_34013249102\",\"title\":\"Paragraph 1 title\",\"script\":{\"text\":\"%md Hello world!\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"SUCCESS\",\"msg\":[]},\"settings\":{},\"text\":\"%md Hello world!\",\"config\":{}},{\"id\":\"paragraph_65324234523\",\"title\":\"Paragraph 2 title\",\"script\":{\"text\":\"%md Hello another paragraph!\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"SUCCESS\",\"msg\":[]},\"settings\":{},\"text\":\"%md Hello another paragraph!\",\"config\":{}},{\"id\":\"paragraph_4362340312\",\"title\":\"This paragraph will cause an error\",\"script\":{\"text\":\"goofy script that causes an error\",\"interpreter\":{\"className\":\"com.teragrep.zep_01.interpreter.EchoInterpreter\",\"properties\":{\"zeppelin.interpreter.echo.fail\":\"true\"},\"defaultInterpreterGroup\":\"echo\"}},\"results\":{\"code\":\"SUCCESS\",\"msg\":[]},\"settings\":{},\"text\":\"goofy script that causes an error\",\"config\":{}}]}",json.toString());
            });
        }
    @Test
    public void moveParagraphBack() {
        assertNoExceptionIsThrown(()->{
// Ensure that paragraphs are in preset order before the test
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(), paragraphId1);
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(), paragraphId2);
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(), errorParagraphId);

            // Move last paragraph backwards to first index
            Notebook reorderedParagraph = notebook.reorderParagraph(errorParagraphId, 0);

            // Ensure that paragraphs are in expeceted order after the test
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(), errorParagraphId);
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(), paragraphId1);
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(), paragraphId2);

            // Move the middle paragraph backwards to first index after the first operation completes
            Notebook reorderedParagraph2 = reorderedParagraph.reorderParagraph(paragraphId1, 0);

            // Ensure that paragraphs are in expeceted order after the test
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(), paragraphId1);
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(), errorParagraphId);
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(), paragraphId2);
        });
        }

    @Test
    public void moveParagraphForward(){
        assertNoExceptionIsThrown(()->{
            // Ensure that paragraphs are in preset order before the test
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(), paragraphId1);
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(), paragraphId2);
            Assert.assertEquals(notebook.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(), errorParagraphId);

            // Move first paragraph forwards to second index
            Notebook reorderedParagraph = notebook.reorderParagraph(paragraphId1,1);

            // Ensure that paragraphs are in expeceted order after the test
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(),paragraphId2);
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(),paragraphId1);
            Assert.assertEquals(reorderedParagraph.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(),errorParagraphId);

            // Move the first paragraph forwards to last index after the first operation completes
            Notebook reorderedParagraph2 = reorderedParagraph.reorderParagraph(paragraphId2,2);

            // Ensure that paragraphs are in expeceted order after the test
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(0).id(),paragraphId1);
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(1).id(),errorParagraphId);
            Assert.assertEquals(reorderedParagraph2.paragraphs().values().stream().collect(Collectors.toList()).get(2).id(),paragraphId2);
        });
    }
}