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
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.InterpreterContext;

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;

public final class DTTableDatasetNg extends AbstractUserInterfaceElement {
    // FIXME Exceptions should cause interpreter to stop

    private final Lock lock = new ReentrantLock();

    private final AngularObject<String> AJAXRequestAngularObject;

    private List<String> datasetAsJSON = null;
    private JsonArray schemaHeadersJson;

    /*
    currentAJAXLength is shared between all the clients when server refreshes
    perhaps we could just let the clients know that there is an update and
    that they would each request their own copy and the request would contain
    the size?
     */
    private int currentAJAXLength = 25;

    public DTTableDatasetNg(InterpreterContext interpreterContext) {
        super(interpreterContext);
        AJAXRequestAngularObject = getInterpreterContext().getAngularObjectRegistry().add(
                "AJAXRequest_"+getInterpreterContext().getParagraphId(),
                "{}",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );
        schemaHeadersJson = Json.createArrayBuilder().build();
        AJAXRequestAngularObject.addWatcher(new AJAXRequestWatcher(interpreterContext, this));
    }

    void refreshPage() {
        try {
            lock.lock();
                Method updateAllResultMessagesMethod = InterpreterOutput.class.getDeclaredMethod("updateAllResultMessages");
                updateAllResultMessagesMethod.setAccessible(true);
                updateAllResultMessagesMethod.invoke(getInterpreterContext().out);
        }catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e){
            LOGGER.error(e.toString());
        } finally {
            lock.unlock();
        }
    }

    void handeAJAXRequest(JsonObject ajaxRequest) {
        try {
            lock.lock();
            // Apply defaults if parameters are not provided
            int start = ajaxRequest.getJsonNumber("start").intValue();
            int length = ajaxRequest.getJsonNumber("length").intValue();
            int draw = ajaxRequest.getJsonNumber("draw").intValue();
            String searchString = ajaxRequest.getJsonObject("search").getString("value");
            updatePage(start,length,searchString, draw, false);
        }
        finally {
            lock.unlock();
        }

    }

    @Override
    public void draw() {
    }

    @Override
    public void emit() {
        try {
            lock.lock();
            AJAXRequestAngularObject.emit();

        } finally {
            lock.unlock();
        }
    }

    public void setParagraphDataset(Dataset<Row> rowDataset) {
        /*
         TODO check if other presentation can be used than string, for order
         i.e. rowDataset.collectAsList()
         */

        try {
            lock.lock();
            if (rowDataset.schema().nonEmpty()) {
                // needs to be here as sparkContext might disappear later
                DTHeader dtHeader = new DTHeader(rowDataset.schema());
                schemaHeadersJson = dtHeader.json();
                datasetAsJSON = rowDataset.toJSON().collectAsList();
                updatePage(0,currentAJAXLength,"",1,true);
            }
        } finally {
            lock.unlock();
        }
    }

    // Sends a PARAGRAPH_UPDATE_OUTPUT message to UI containing the paginated data
    private void updatePage(int start, int length, String searchString, int draw, boolean clearParagraphResults){
        if (datasetAsJSON == null) {
            LOGGER.warn("attempting to draw empty dataset");
            return;
        }
        try {
            JsonObject response = SearchAndPaginate(draw, start,length,searchString);
            String outputContent = "%jsontable\n" +
                    response.toString();
            getInterpreterContext().out().clear(clearParagraphResults);
            getInterpreterContext().out().write(outputContent);
            getInterpreterContext().out().flush();
        } catch (java.io.IOException e) {
            LOGGER.error(e.toString());
        }
    }

    private JsonObject SearchAndPaginate(int draw, int start, int length, String searchString){
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

        int recordsTotal = datasetAsJSON.size();
        int recordsFiltered = searchedList.size();

        return DTNetResponse(formated, schemaHeadersJson, draw, recordsTotal,recordsFiltered);
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

}
