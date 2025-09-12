/*
 * Teragrep DPL Spark Integration PTH-07
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
package com.teragrep.pth_07.ui.elements.table_dynamic;

import com.teragrep.zep_01.display.AngularObjectWatcher;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;

public class AJAXRequestWatcher extends AngularObjectWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(AJAXRequestWatcher.class);

    private DTTableDatasetNg dtTableDatasetNg;

    public AJAXRequestWatcher(InterpreterContext context) {
        super(context);
    }

    public AJAXRequestWatcher(InterpreterContext context, DTTableDatasetNg dtTableDatasetNg) {
        super(context);
        this.dtTableDatasetNg = dtTableDatasetNg;
    }

    @Override
    public void watch(Object o, Object o1, InterpreterContext interpreterContext) {
        assert(dtTableDatasetNg != null);

        if(LOGGER.isTraceEnabled()) {
            LOGGER.trace("AJAXRequest <- {}", o.toString());
            LOGGER.trace("AJAXRequest -> {}", o1.toString());
        }
        String requestString = (String) o1;
        try{
            JsonObject ajaxRequest = Json.createReader(new StringReader(requestString)).readObject();
            // validate request by checking that every required key exists and that the type of the key is expected.
            if (ajaxRequest.get("draw") != null
                    && ajaxRequest.get("draw").getValueType() == JsonValue.ValueType.NUMBER
                    && ajaxRequest.get("start") != null
                    && ajaxRequest.get("start").getValueType() == JsonValue.ValueType.NUMBER
                    && ajaxRequest.get("length") != null
                    && ajaxRequest.get("length").getValueType() == JsonValue.ValueType.NUMBER
                    && ajaxRequest.get("search") != null
                    && ajaxRequest.get("search").getValueType() == JsonValue.ValueType.OBJECT
                    && ajaxRequest.getJsonObject("search").get("value") != null
                    && ajaxRequest.getJsonObject("search").get("value").getValueType() == JsonValue.ValueType.STRING
            ) {
                dtTableDatasetNg.handeAJAXRequest(ajaxRequest);
            }
            else {
                LOGGER.error("AJAXRequestWatcher received invalid JSON data: " + ajaxRequest);
            }
        }
        catch (JsonException jsonException){
            LOGGER.error("AJAXRequestWatcher received unparseable JSON data: " + requestString);
        }
    }
}
