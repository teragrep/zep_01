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
package com.teragrep.pth_07.ui.elements;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectWatcher;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.io.StringReader;
import java.util.concurrent.atomic.AtomicReference;

public class PerformanceIndicator extends AbstractUserInterfaceElement {

    private final AngularObject<String> batchMsg;
    private String message;
    private final AtomicReference<Dataset<Row>> performanceData;

    public PerformanceIndicator(InterpreterContext interpreterContext) {
        super(interpreterContext);
        performanceData = new AtomicReference<>();

        AngularObjectWatcher angularObjectWatcher = new AngularObjectWatcher(getInterpreterContext()) {
            @Override
            public void watch(Object o, Object o1, InterpreterContext interpreterContext) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("batchMsg <- {}", o.toString());
                    LOGGER.trace("batchMsg -> {}", o1.toString());
                }
            }
        };

        batchMsg = getInterpreterContext().getAngularObjectRegistry().add(
                "batchMsg",
                "[]",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );
        batchMsg.addWatcher(angularObjectWatcher);

    }

    private void draw(final String message){
        this.message = message;
        draw();
    }

    @Override
    protected void draw() {
        batchMsg.set(message);
    }

    @Override
    public void emit() {
        batchMsg.emit();
    }

    public void setPerformanceDataset(final Dataset<Row> performanceData){
        this.performanceData.set(performanceData);
    }

    public void sendPerformanceUpdate(){
        // TODO: format dataset to uPlot format (requires ZEP_01#283)
        // TODO: format into DataTables format as well if needed (requires ZEP_01#283)
        final JsonArrayBuilder output = Json.createArrayBuilder();
        // Use collect_list to turn the data into a single Spark Row containing JSON strings.
        final Row r = performanceData.get().toJSON().agg(functions.collect_list(functions.col("value")).as("values")).first();
        // Read the Spark Row into a JsonArray
        final JsonArray singleRowPerformanceData = Json.createReader(new StringReader(r.json())).readObject().getJsonArray("values");
        // Iterate over the JsonArray and read the Json Strings into Json Objects. Skipping this step leaves Java's escape character to every quotation mark in the JSON
        for (int i = 0; i < singleRowPerformanceData.size(); i++) {
            String value = singleRowPerformanceData.getString(i);
            JsonObject valueObject = Json.createReader(new StringReader(value)).readObject();
            output.add(valueObject);
        }
        draw(output.build().toString());
    }
}