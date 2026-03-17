package com.teragrep.zep_01.socket.messages;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.*;

import java.util.Objects;

// Server-to-client message that defines the contents of a PARAGRAPH_OUTPUT message.
public final class ParagraphOutputResponseMessage implements Jsonable {

    private final String noteId;
    private final String paragraphId;
    private final JsonObject output;

    public ParagraphOutputResponseMessage(final String noteId, final String paragraphId, final JsonObject output){
        this.noteId = noteId;
        this.paragraphId = paragraphId;
        this.output = output;
    }

    @Override
    public JsonObject asJson() {
        final JsonObject messageData = Json.createObjectBuilder()
                .add("noteId",noteId)
                .add("paragraphId",paragraphId)
                .add("output",output)
                .build();
        return messageData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParagraphOutputResponseMessage that = (ParagraphOutputResponseMessage) o;
        return Objects.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output);
    }
}
