package com.teragrep.zep_01.socket.messages;

import com.teragrep.zep_01.common.Jsonable;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.*;
import jakarta.json.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// CLient-to-Server message that must minimally contain all the required pieces of information for a dataset formatting request.
public final class ParagraphOutputRequestMessage implements Jsonable {

    private final JsonObject json;

    public ParagraphOutputRequestMessage(final JsonObject json){
        this.json = json;
    }

    public String data() throws JsonException {
        if(!json.containsKey("data") || !json.get("data").getValueType().equals(JsonValue.ValueType.OBJECT)){
            throw new JsonException("Json does not contain a msgId!");
        } else {
            return json.getJsonObject("data").toString();
        }
    }

    public String messageId() throws JsonException {
        if (!json.containsKey("msgId") || !json.get("msgId").getValueType().equals(JsonValue.ValueType.STRING)) {
            throw new JsonException("Json does not contain a msgId!");
        } else {
            return json.getString("msgId");
        }
    }

    public String noteId() throws JsonException {
        if(!json.containsKey("data") || !json.get("data").getValueType().equals(JsonValue.ValueType.OBJECT)) {
            throw new JsonException("Json does not contain a data object!");
        }
        else{
            final JsonObject data = json.getJsonObject("data");
            if (!data.containsKey("noteId") || !data.get("noteId").getValueType().equals(JsonValue.ValueType.STRING)) {
                throw new JsonException("Json does not contain a noteId!");
            } else {
                return data.getString("noteId");
            }
        }
    }

    public String paragraphId() throws JsonException {
            if(!json.containsKey("data") || !json.get("data").getValueType().equals(JsonValue.ValueType.OBJECT)) {
                throw new JsonException("Json does not contain a data object!");
            }
            else {
                final JsonObject data = json.getJsonObject("data");
                if (!data.containsKey("paragraphId") || !data.get("paragraphId").getValueType().equals(JsonValue.ValueType.STRING)) {
                    throw new JsonException("Json does not contain a paragraphId!");
                } else {
                    return data.getString("paragraphId");
                }
            }
    }
    public String type() throws JsonException {
            if(!json.containsKey("data") || !json.get("data").getValueType().equals(JsonValue.ValueType.OBJECT)){
                throw new JsonException("Json does not contain a data object!");
            }
            else {
                final JsonObject data = json.getJsonObject("data");
                if (!data.containsKey("type") || !data.get("type").getValueType().equals(JsonValue.ValueType.STRING)) {
                    throw new JsonException("Json does not contain a type!");
                } else {
                    return data.getString("type");
                }
            }
        }


    @Override
    public JsonObject asJson() {
        return json;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ParagraphOutputRequestMessage that = (ParagraphOutputRequestMessage) o;
        return Objects.equals(json, that.json);
    }

    @Override
    public int hashCode() {
        return Objects.hash(json);
    }
}
