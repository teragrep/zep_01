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

import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.DataTablesFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DataTablesFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DatasetFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.Order;
import com.teragrep.pth_07.ui.elements.AbstractUserInterfaceElement;
import com.teragrep.zep_01.interpreter.InterpreterException;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DTTableDatasetNg extends AbstractUserInterfaceElement {
    // FIXME Exceptions should cause interpreter to stop

    private final Lock lock = new ReentrantLock();
    private Dataset<Row> dataset = null;
    private Dataset<String> datasetAsJson = null;
    private DTHeader schemaHeaders;
    private int drawCount;

    /*
    currentAJAXLength is shared between all the clients when server refreshes
    perhaps we could just let the clients know that there is an update and
    that they would each request their own copy and the request would contain
    the size?
     */
    private int currentAJAXLength = 50;

    public DTTableDatasetNg(final InterpreterContext interpreterContext) {
        this(interpreterContext, new DTHeader(), 1);
    }

    public DTTableDatasetNg(final InterpreterContext interpreterContext, final DTHeader schemaHeaders, final int drawCount){
        super(interpreterContext);
        this.schemaHeaders = schemaHeaders;
        this.drawCount = drawCount;
    }
    @Override
    public void draw() {
    }

    @Override
    public void emit() {
    }

    public void setParagraphDataset(Dataset<Row> rowDataset) {
        /*
         TODO check if other presentation can be used than string, for order
         i.e. rowDataset.collectAsList()
         */

        try {
            lock.lock();
            // Reset draw when schema changes
            if(!schemaHeaders.schema().equals(rowDataset.schema())){
                drawCount = 1;
            }
            // Increment draw when schema has not changed.
            else {
                drawCount++;
            }

            // unpersist dataset upon receiving new data
            if(datasetAsJson != null){
                datasetAsJson.unpersist();
            }
            if(dataset != null){
                dataset.unpersist();
            }
            if (rowDataset.schema().nonEmpty()) {
                // needs to be here as sparkContext might disappear later
                dataset = rowDataset.persist(StorageLevel.MEMORY_AND_DISK());
                datasetAsJson = rowDataset.toJSON().persist(StorageLevel.MEMORY_AND_DISK());
                schemaHeaders = new DTHeader(rowDataset.schema());
            }
        } finally {
            lock.unlock();
        }
    }

    private void write(String outputContent){
        try {
            getInterpreterContext().out().clear(false);
            getInterpreterContext().out().write(outputContent);
            getInterpreterContext().out().flush();
        } catch (IOException e) {
            LOGGER.error(e.toString());
            e.printStackTrace();
        }
    }

    public Dataset<Row> dataset(){
        return dataset;
    }
    public void writeDataUpdate() throws InterpreterException{
        Map<String, String> defaultOptions = new HashMap<String, String>();
        defaultOptions.put("draw",Integer.toString(drawCount));
        defaultOptions.put("start",Integer.toString(0));
        defaultOptions.put("length",Integer.toString(currentAJAXLength));
        defaultOptions.put("search","");
        writeDataUpdate(new DataTablesFormat(dataset,new DataTablesFormatOptions(defaultOptions)));
    }

    public void writeDataUpdate(DatasetFormat format) throws InterpreterException{
            JsonObject formatted = format.format();
            String outputContent = "%"+format.type().toLowerCase()+"\n" +
                    formatted.toString();
            write(outputContent);

    }

    static JsonArray dataStreamParser(List<String> data){

        try{
            JsonArrayBuilder builder = Json.createArrayBuilder();


            for (String S : data) {
                JsonReader reader = Json.createReader(new StringReader(S));
                JsonObject line = reader.readObject();
                builder.add(line);
                reader.close();
            }
            return builder.build();
        }catch(JsonException|IllegalStateException e){
            LOGGER.error(e.toString());
            return(Json.createArrayBuilder().build());
        }
    }

    static JsonObject DTNetResponse(JsonArray data, JsonArray schemaHeadersJson, int draw, int recordsTotal, int recordsFiltered){
        try{
            JsonObjectBuilder builder = Json.createObjectBuilder();
            builder.add("headers",schemaHeadersJson);
            builder.add("data", data);
            builder.add("draw", draw);
            builder.add("recordsTotal", recordsTotal);
            builder.add("recordsFiltered", recordsFiltered);
            return builder.build();
        }catch(JsonException|IllegalStateException e){
            LOGGER.error(e.toString());
            return(Json.createObjectBuilder().build());
        }
    }

    public List<String> getDatasetAsJSON(){
        return datasetAsJson.collectAsList();
    }
}
