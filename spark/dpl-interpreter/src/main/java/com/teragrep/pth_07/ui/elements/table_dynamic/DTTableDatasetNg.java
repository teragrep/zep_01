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

import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.Order;
import com.teragrep.pth_07.ui.elements.AbstractUserInterfaceElement;
import com.teragrep.zep_01.interpreter.InterpreterException;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.interpreter.InterpreterContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DTTableDatasetNg extends AbstractUserInterfaceElement {
    // FIXME Exceptions should cause interpreter to stop

    private final Lock lock = new ReentrantLock();
    private Dataset<Row> dataset = null;
    private List<String> datasetAsJSON = null;
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
            if (rowDataset.schema().nonEmpty()) {
                // needs to be here as sparkContext might disappear later
                dataset = rowDataset;
                schemaHeaders = new DTHeader(rowDataset.schema());
                datasetAsJSON = rowDataset.toJSON().collectAsList();
            }
        } finally {
            lock.unlock();
        }
    }

    public void writeRawDataupdate(){
        writeRawDataupdate(0,currentAJAXLength,"",drawCount);
    }
    // Sends a PARAGRAPH_UPDATE_OUTPUT message to UI containing the formatted data received from BatchHandler.
    private void writeRawDataupdate(int start, int length, String searchString, int draw){
        try {
            JsonObject response = SearchAndPaginate(draw, start,length,searchString);
            String outputContent = "%jsontable\n" +
                    response.toString();
            write(outputContent, true);
        } catch (InterpreterException ie){
            LOGGER.error("Failed to draw pagination request!",ie);
        }
    }

    public void writeAggregatedDataupdate(){
        writeAggregatedDataupdate("DataTables","table");
    }

    public void writeAggregatedDataupdate(String libraryName, String chartType){
        JsonArray data = dataStreamParser(datasetAsJSON);
        final JsonArray schemaHeadersAsJSON = schemaHeaders.json();
        JsonObject response = DTNetResponse(data,schemaHeadersAsJSON,drawCount,datasetAsJSON.size(),datasetAsJSON.size());

        String outputContent = "%jsontable\n" +
                response.toString();
        write(outputContent, false);
    }

    private void write(String outputContent, boolean flush){
        try {
            getInterpreterContext().out().clear(false);
            getInterpreterContext().out().write(outputContent);
            if(flush){
                getInterpreterContext().out().flush();
            }
        } catch (IOException e) {
            LOGGER.error(e.toString());
            e.printStackTrace();
        }
    }

    public Dataset<Row> dataset(){
        return dataset;
    }

    public JsonObject SearchAndPaginate(int draw, int start, int length, String searchString) throws InterpreterException {
        if(datasetAsJSON == null){
            throw new InterpreterException("Attempting to draw an empty dataset!");
        }
        DTSearch dtSearch = new DTSearch(datasetAsJSON);
        List<Order> currentOrder = null;

        // TODO these all decode the JSON, it refactor therefore to decode only once
        // searching
        List<String> searchedList = dtSearch.search(searchString);

        // TODO ordering
        //DTOrder dtOrder = new DTOrder(searchedList);
        //List<String> orderedlist = dtOrder.order(searchedList, currentOrder);
        List<String> orderedlist = searchedList;

        // pagination
        DTPagination dtPagination = new DTPagination(orderedlist);
        List<String> paginatedList = dtPagination.paginate(length, start);

        // ui formatting
        JsonArray formated = dataStreamParser(paginatedList);
        final JsonArray schemaHeadersAsJSON = schemaHeaders.json();
        int recordsTotal = datasetAsJSON.size();
        int recordsFiltered = searchedList.size();

        return DTNetResponse(formated, schemaHeadersAsJSON, draw, recordsTotal,recordsFiltered);
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
        if(datasetAsJSON == null){
            return new ArrayList<>();
        }
        return datasetAsJSON;
    }
}
