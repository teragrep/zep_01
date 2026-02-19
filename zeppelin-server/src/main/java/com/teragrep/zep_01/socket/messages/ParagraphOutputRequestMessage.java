package com.teragrep.zep_01.socket.messages;

import com.teragrep.zep_01.common.Jsonable;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.*;
import jakarta.json.*;

import java.util.ArrayList;
import java.util.List;

// CLient-to-Server message that must minimally contain all the required pieces of information for a dataset formatting request.
public class ParagraphOutputRequestMessage implements Jsonable {

    private final JsonObject json;

    public ParagraphOutputRequestMessage(JsonObject json){
        this.json = json;
    }

    public String messageId() throws JsonException {
        if (!json.containsKey("msgId") || !json.get("msgId").getValueType().equals(JsonValue.ValueType.STRING)) {
            throw new JsonException("Json does not contain a msgId!");
        } else {
            return json.getString("msgId");
        }
    }

    public String noteId() throws JsonException {
        if (!json.containsKey("noteId") || !json.get("noteId").getValueType().equals(JsonValue.ValueType.STRING)) {
            throw new JsonException("Json does not contain a noteId!");
        } else {
            return json.getString("noteId");
        }
    }

    public String paragraphId() throws JsonException {
        if (!json.containsKey("paragraphId") || !json.get("paragraphId").getValueType().equals(JsonValue.ValueType.STRING)) {
            throw new JsonException("Json does not contain a paragraphId!");
        } else {
            return json.getString("paragraphId");
        }
    }
    public String visualizationLibraryName() throws JsonException {
        if (!json.containsKey("type") || !json.get("type").getValueType().equals(JsonValue.ValueType.STRING)) {
            throw new JsonException("Json does not contain a type!");
        } else {
            return json.getString("type");
        }
    }

    public Options options() throws JsonException{
        String type = visualizationLibraryName();
        Options options;
        if(type.equals(InterpreterResult.Type.DATATABLES.label)){
            if(!json.containsKey("requestOptions") || !json.get("requestOptions").getValueType().equals(JsonValue.ValueType.OBJECT)){
                throw new JsonException("Json does not contain requestOptions object!");
            }
            options = Options.dataTablesOptions(dataTablesOptions(json.getJsonObject("requestOptions")));
        }
        else if(type.equals(InterpreterResult.Type.UPLOT.label)){
            if(!json.containsKey("requestOptions") || !json.get("requestOptions").getValueType().equals(JsonValue.ValueType.OBJECT)){
                throw new JsonException("Json does not contain requestOptions object!");
            }
            options = Options.uPlotOptions(uplotOptions(json.getJsonObject("requestOptions")));
        }
        else {
            throw new JsonException("RequestOptions is in invalid format for DataTables!");
        }
        return options;
    }

    // Verifies that all keys are present and in correct format. If so, returns a DataTablesOptions object
    private DataTablesOptions dataTablesOptions(JsonObject json){
        if(!json.containsKey("draw") || !json.get("draw").getValueType().equals(JsonValue.ValueType.NUMBER)){
            throw new JsonException("Json does not contain a draw number!");
        }
        int draw = json.getInt("draw");

        if(!json.containsKey("start") || !json.get("start").getValueType().equals(JsonValue.ValueType.NUMBER)){
            throw new JsonException("Json does not contain a start number!");
        }
        int start = json.getInt("start");

        if(!json.containsKey("length") || !json.get("length").getValueType().equals(JsonValue.ValueType.NUMBER)){
            throw new JsonException("Json does not contain a length number!");
        }
        int length = json.getInt("length");

        if(!json.containsKey("search") || !json.get("search").getValueType().equals(JsonValue.ValueType.OBJECT)){
            throw new JsonException("Json does not contain a search object!");
        }
        DataTablesSearch search = dataTablesSearch(json.getJsonObject("search"));

        if(!json.containsKey("order") || !json.get("order").getValueType().equals(JsonValue.ValueType.ARRAY)){
            throw new JsonException("Json does not contain a order array!");
        }
        JsonArray orderJson = json.getJsonArray("order");
        List<String> order = new ArrayList<>();
        for (int i = 0; i < orderJson.size(); i++) {
            if(!orderJson.get(i).getValueType().equals(JsonValue.ValueType.STRING)){
                throw new JsonException("Order array does not contain only Strings!");
            }
            order.add(orderJson.getString(i));
        }

        if(!json.containsKey("columns") || !json.get("columns").getValueType().equals(JsonValue.ValueType.ARRAY)){
            throw new JsonException("Json does not contain a columns array!");
        }
        JsonArray columnsJson = json.getJsonArray("columns");
        List<DataTablesColumns> columns = new ArrayList<>();
        for (int i = 0; i < columnsJson.size(); i++) {
            if(!columnsJson.get(i).getValueType().equals(JsonValue.ValueType.OBJECT)){
                throw new JsonException("Columns array does not contain only Objects!");
            }
            columns.add(dataTablesColumns(columnsJson.getJsonObject(i)));
        }
        DataTablesOptions dataTablesOptions = new DataTablesOptions(draw,start,length,search,order,columns);
        return dataTablesOptions;
    }

    private DataTablesSearch dataTablesSearch(JsonObject json){
        if(!json.containsKey("value") || !json.get("value").getValueType().equals(JsonValue.ValueType.STRING)){
            throw new JsonException("Json does not contain search.value string!");
        }
        if(!json.containsKey("regex") || !(json.get("regex").getValueType().equals(JsonValue.ValueType.TRUE) || json.get("regex").getValueType().equals(JsonValue.ValueType.FALSE))){
            throw new JsonException("Json does not contain search.regex boolean!");
        }
        if(!json.containsKey("fixed") || !json.get("fixed").getValueType().equals(JsonValue.ValueType.ARRAY)){
            throw new JsonException("Json does not contain search.fixed array!");
        }
        List<String> fixed = new ArrayList<>();
        JsonArray fixedJson = json.getJsonArray("fixed");
        for (int i = 0; i < fixedJson.size(); i++) {
            if(!fixedJson.get(i).getValueType().equals(JsonValue.ValueType.STRING)){
                throw new JsonException("search.fixed does not contain only string values!");
            }
            fixed.add(fixedJson.getString(i));
        }
        return new DataTablesSearch(json.getString("value"), json.getBoolean("regex"), fixed);
    }

    private DataTablesColumns dataTablesColumns(JsonObject json){
        if(!json.containsKey("name") || !(json.get("name").getValueType().equals(JsonValue.ValueType.STRING))){
            throw new JsonException("Json does not contain column.name string!");
        }
        String name = json.getString("name");

        if(!json.containsKey("data") || !(json.get("data").getValueType().equals(JsonValue.ValueType.STRING))){
            throw new JsonException("Json does not contain column.data string!");
        }
        String data = json.getString("data");

        if(!json.containsKey("searchable") || !(json.get("searchable").getValueType().equals(JsonValue.ValueType.TRUE) || json.get("searchable").getValueType().equals(JsonValue.ValueType.FALSE))){
            throw new JsonException("Json does not contain column.searchable boolean!");
        }
        boolean searchable = json.getBoolean("searchable");

        if(!json.containsKey("orderable") || !(json.get("orderable").getValueType().equals(JsonValue.ValueType.TRUE) || json.get("orderable").getValueType().equals(JsonValue.ValueType.FALSE))){
            throw new JsonException("Json does not contain column.orderable boolean!");
        }
        boolean orderable = json.getBoolean("orderable");

        if(!json.containsKey("search") || !json.get("search").getValueType().equals(JsonValue.ValueType.OBJECT)){
            throw new JsonException("Json does not contain a columns.search object!");
        }
        DataTablesSearch search = dataTablesSearch(json.getJsonObject("search"));
        return new DataTablesColumns(data,name,orderable,search,searchable);
    }

    private UPlotOptions uplotOptions(JsonObject json){
        if(!json.containsKey("graphType") || !json.get("graphType").getValueType().equals(JsonValue.ValueType.STRING)){
            throw new JsonException("Json does not contain a graphType string!");
        }
        String graphType = json.getString("graphType");
        UPlotOptions uPlotOptions = new UPlotOptions(graphType);
        return uPlotOptions;
    }


    @Override
    public JsonObject asJson() {
        return json;
    }
}
