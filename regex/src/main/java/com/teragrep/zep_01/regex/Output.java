/*
 * Regex interpreter for Teragrep
 * Copyright (C) 2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.zep_01.regex;

import com.teragrep.zep_01.regex.captureGroup.Jsonable;
import jakarta.json.*;
import jakarta.json.stream.JsonGenerator;

import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

public class Output implements Jsonable {
    private static final JsonWriterFactory writerFactory = Json.createWriterFactory(
            Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

    private final String regex;
    private final List<Jsonable> jsonableGroups;

    public Output(final String regex, List<Jsonable> jsonableGroups) {
        this.regex = regex;
        this.jsonableGroups = jsonableGroups;
    }

    @Override
    public JsonObject toJson() {
        final JsonObjectBuilder recordSchemaBuilder = Json.createObjectBuilder();

        recordSchemaBuilder.addNull("recordType");

        recordSchemaBuilder.add("regex", regex);

        final JsonArrayBuilder recordSchemeDataBuilder = Json.createArrayBuilder();

        // produce describable key value where describer is a decoration that can auto-analyze?
        for (Jsonable jsonable : jsonableGroups) {
            recordSchemeDataBuilder.add(jsonable.toJson());
        }

        recordSchemaBuilder.add("columns", recordSchemeDataBuilder.build());

        return recordSchemaBuilder.build();
    }

    @Override
    public String toString() {
        final StringWriter stringWriter = new StringWriter();
        try (JsonWriter jsonWriter = writerFactory.createWriter(stringWriter)) {
            jsonWriter.writeObject(toJson());
        }
        return stringWriter.toString();

    }

}
