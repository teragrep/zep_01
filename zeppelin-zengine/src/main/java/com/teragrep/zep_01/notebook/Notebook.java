package com.teragrep.zep_01.notebook;


import com.teragrep.zep_01.notebook.repo.ZeppelinFile;
import com.teragrep.zep_01.notebook.repo.Directory;
import jakarta.json.*;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.notebook.repo.ByteOrderMarkRemoved;
import com.teragrep.zep_01.interpreter.*;
import com.teragrep.zep_01.notebook.utility.IdHashes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a single Notebook within Zeppelin
 */
public final class Notebook extends ZeppelinFile implements Stubable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Notebook.class);
  private final LinkedHashMap<String,Paragraph> paragraphs;
  private final String title;

  // Compatibility fields, remove these once Interpreter is refactored
  private final ExecutionContext executionContext;
  // End compatibility fields
  public Notebook(String title, String id, Path path, LinkedHashMap<String,Paragraph> paragraphs) {
    super(id,path);
    this.title = title;
    this.paragraphs = paragraphs;

    // Compatibility fields, remove once Interpreter is refactored
    this.executionContext = new ExecutionContext();
    this.executionContext.setNoteId(id);
    this.executionContext.setDefaultInterpreterGroup(ZeppelinConfiguration.create().getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT));
    // End compatibility fields
  }

  // Remove file from disk
  @Override
  public void delete() throws IOException {
    Files.delete(path());
  }

  // Checks if searched ID matches with this Notebooks ID.
  @Override
  public ZeppelinFile findFile(String id) throws FileNotFoundException {
    if(id().equals(id)){
      return this;
    }
    else {
      throw new FileNotFoundException("Searched id "+id+" does not match with"+id());
    }
  }

  // Checks if searched path matches with this Notebooks path.
  @Override
  public ZeppelinFile findFile(Path path) throws FileNotFoundException {
    if(path().equals(path)){
      return this;
    }
    else {
      throw new FileNotFoundException("Searched path "+path+" does not match with"+path());
    }
  }

  public String title() {
    return title;
  }
  public LinkedHashMap<String,Paragraph> paragraphs(){
    return paragraphs;
  }

  // Executes every Paragraph in order.
  public Notebook runAll() throws Exception {
    LinkedHashMap<String,Paragraph> executedParagraphs = new LinkedHashMap();
    for (Paragraph paragraph : paragraphs.values()) {
      executedParagraphs.put(paragraph.id(),new Paragraph(paragraph.id(),paragraph.title(),paragraph.run(),paragraph.script()));
    }
    return new Notebook(title,id(),path(),executedParagraphs);
  }

  public JsonObject json(){
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add("id",id());
    builder.add("name", title);
    //compatibility fields//
    builder.add("config",Json.createObjectBuilder(new HashMap<>()).build());
    // end //
    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

    for (Paragraph paragraph:paragraphs.values()
    ) {
      arrayBuilder.add(paragraph.json());
    }
    JsonArray paragraphJsonArray = arrayBuilder.build();
    builder.add("paragraphs",paragraphJsonArray);
    return builder.build();
  }
  public Notebook copy(Path path, String id) throws IOException {
    return copy(title,path,id);
  }

  @Override
  public Map<String, ZeppelinFile> children() {
    return new HashMap<>();
  }

  @Override
  public Map<String, NoteInfo> toNoteInfo(HashMap<String, NoteInfo> noteInfoMap, Path rootDir) {
    noteInfoMap.put(id(),new FormattedNoteInfo(id(),rootDir.relativize(path()).toString()));
    return noteInfoMap;
  }

  @Override
  public void printTree() {
    System.out.println("File ID: "+id()+", Path: "+path());
    LOGGER.debug("File ID: {}, Path: {}",id(),path());
  }

  @Override
  public Notebook load() throws IOException {
    String content = new ByteOrderMarkRemoved(readFile()).toString();
    StringReader stringReader = new StringReader(content);
    JsonReader jsonReader = Json.createReader(stringReader);
    JsonObject object = jsonReader.readObject();
    jsonReader.close();
    stringReader.close();

    String name = object.getString("name");
    String id = object.getString("id");
    JsonArray paragraphJsonArray = object.getJsonArray("paragraphs");
    LinkedHashMap<String, Paragraph> paragraphs = new LinkedHashMap<>();
    for (JsonObject paragraphJson:paragraphJsonArray.getValuesAs(JsonObject.class)
    ) {
      NullParagraph nullParagraph = new NullParagraph();
      Paragraph paragraph = nullParagraph.fromJson(paragraphJson);
      paragraphs.put(paragraph.id(),paragraph);
    }
    return new Notebook(name,id,path(),paragraphs);
  }

  @Override
  public List<ZeppelinFile> listAllChildren() {
    return new ArrayList<>();
  }

  public Notebook copy(String title, Path path, String id) throws IOException {
    LinkedHashMap<String, Paragraph> copyParagraphs = new LinkedHashMap<String,Paragraph>();
    for (Paragraph paragraph: paragraphs.values()) {
      String copyParagraphId = IdHashes.generateId();
      InterpreterContext copyInterpreterContext = InterpreterContext.builder()
              .setParagraphId(copyParagraphId)
              .setNoteId(id)
              .setParagraphText(paragraph.script().interpreterContext().getParagraphText())
              .setReplName(paragraph.script().interpreterContext().getReplName())
              .build();
      Script copyScript = new Script(paragraph.script().text(),paragraph.script().interpreter(),copyInterpreterContext);
      Result copyResult = new Result(paragraph.result().code(),paragraph.result().messages());
      Paragraph copyParagraph = new Paragraph(copyParagraphId,paragraph.title(),copyResult,copyScript);
      copyParagraphs.put(copyParagraph.id(),copyParagraph);
    }
    Notebook copyNotebook = new Notebook(title,id,path,copyParagraphs);
    copyNotebook.save();
    return copyNotebook;
  }

  public void save() throws IOException {
    try{
      StringWriter stringWriter = new StringWriter();
      Files.createDirectories(path().getParent());
      Files.write(path(), json().toString().getBytes());
      stringWriter.close();
    }
    catch (IOException exception){
      throw new IOException("Failed to save notebook to path"+path()+"!",exception);
    }
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  public void rename(String fileName) throws IOException {
    move(Paths.get(path().getParent().toString(),fileName));
  }

  public void move(Path path) throws IOException {
    Notebook movedNotebook = copy(path,id());
    movedNotebook.save();
    delete();
  }
  public Notebook move(Directory parentDirectory) throws IOException {
    return move(parentDirectory,path().getFileName().toString());
  }
  public Notebook move(Directory parentDirectory, String fileName) throws IOException {
    Notebook movedNotebook = copy(Paths.get(parentDirectory.path().toString(),fileName),id());
    movedNotebook.save();
    delete();
    return movedNotebook;
  }

  public ExecutionContext executionContext(){
    return executionContext;
  }

  // Move a paragraph to a given index.
  // As paragraphs are stored in a Map, their order is
  public Notebook reorderParagraph(String paragraphId, int index){
    Paragraph paragraphToMove = paragraphs.get(paragraphId);
    ArrayList<Paragraph> paragraphlist = new ArrayList<>(paragraphs.values());
    int currentIndex = paragraphlist.indexOf(paragraphToMove);
    int swapsNeeded = index - currentIndex;

    if(swapsNeeded == 0){
      return this;
    }

    if(swapsNeeded < 0){
      // Move backwards
      for (int swapsPerformed = 0; swapsPerformed < Math.abs(swapsNeeded); swapsPerformed++) {
        Collections.swap(paragraphlist,currentIndex,currentIndex-1);
        currentIndex--;
      }
    }

    else {
      // Move forwards
      for (int swapsPerformed = 0; swapsPerformed < Math.abs(swapsNeeded); swapsPerformed++) {
        Collections.swap(paragraphlist,currentIndex,currentIndex+1);
        currentIndex++;
      }
    }

    LinkedHashMap<String,Paragraph> reorderedParagraphs = new LinkedHashMap<>();
    for (Paragraph paragraph:paragraphlist) {
      reorderedParagraphs.put(paragraph.id(),paragraph);
    }
    Notebook reorderedNotebook = new Notebook(this.title,id(),this.path(),reorderedParagraphs);
    return reorderedNotebook;
  }
  public boolean isStub(){
    return false;
  }
  public String readFile() throws IOException {
    List<String> lines = Files.readAllLines(path(), Charset.defaultCharset());
    String concatenatedLines = lines.stream()
            .map(n -> String.valueOf(n))
            .collect(Collectors.joining("\n"));
    return concatenatedLines;
  }
}
