package com.teragrep.zep_01.notebook;


import com.teragrep.zep_01.notebook.repo.ByteOrderMarkRemoved;
import com.teragrep.zep_01.notebook.repo.ZeppelinFile;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import com.teragrep.zep_01.user.AuthenticationInfo;
import jakarta.json.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents an unloaded notebook.
 * Can only report its name and ID. Other operatoins require you to load the notebook from the Path.
 */
public final class UnloadedNotebook extends ZeppelinFile {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadedNotebook.class);
  private final Map<String,Paragraph> paragraphs;

  public UnloadedNotebook(String id, Path path) {
    super(id,path);
    this.paragraphs = new HashMap<>();
  }

  @Override
  public void delete() throws IOException {
    Files.delete(path());
  }

  @Override
  public ZeppelinFile findFile(String id) throws FileNotFoundException {
    if(id.equals(id())){
      return this;
    }
    else {
      throw new FileNotFoundException("Searched id "+id+" does not match with"+id());
    }
  }

  @Override
  public ZeppelinFile findFile(Path path) throws FileNotFoundException {
    if(path().equals(path)){
      return this;
    }
    else {
      throw new FileNotFoundException("Searched path "+path+" does not match with"+path());
    }
  }

  public String id() {
    return super.id();
  }

  public String title() {
    throw new UnsupportedOperationException("UnloadedNotebook can have no title!");
  }
  public Map<String,Paragraph> paragraphs(){
    throw new UnsupportedOperationException("UnloadedNotebook can have no paragraphs!");
  }
  public UnloadedNotebook runAll(AuthenticationInfo authInfo) throws Exception {
    throw new UnsupportedOperationException("Cannot run UnloadedNotebook!");
  }

  public JsonObject json(){
    throw new UnsupportedOperationException("Cannot turn UnloadedNotebook into JSON!");
  }

  @Override
  public UnloadedNotebook copy(Path path, String id) throws IOException {
    throw new UnsupportedOperationException("Cannot copy UnloadedNotebook!");
  }

  @Override
  public Map<String, ZeppelinFile> children() {
    return new HashMap<>();
  }

  @Override
  public HashMap<String, NoteInfo> toNoteInfo(HashMap<String,NoteInfo> noteInfoMap, Path rootDir){
    noteInfoMap.put(id(),new FormattedNoteInfo(id(),rootDir.relativize(path()).toString()));
    return noteInfoMap;
  }

  @Override
  public void printTree() {
    throw new UnsupportedOperationException("Cannot print tree on a UnloadedNotebook!");
  }

  @Override
  public String readFile() throws IOException {
    List<String> lines = Files.readAllLines(path(), Charset.defaultCharset());
    String concatenatedLines = lines.stream()
            .map(n -> String.valueOf(n))
            .collect(Collectors.joining("\n"));
    return concatenatedLines;
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
  public void move(Path path) throws IOException {
    throw new UnsupportedOperationException("Cannot move an Unloaded Notebook!");
  }

  @Override
  public void rename(String name) throws IOException {
    throw new UnsupportedOperationException("Cannot rename an Unloaded Notebook!");
  }

  @Override
  public List<ZeppelinFile> listAllChildren() {
    return new ArrayList<>();
  }


  @Override
  public void save() throws IOException {
    throw new UnsupportedOperationException("Cannot save a UnloadedNotebook!");
  }

  @Override
  public boolean isDirectory() {
    return false;
  }


  public boolean isStub() {
    return true;
  }

}
