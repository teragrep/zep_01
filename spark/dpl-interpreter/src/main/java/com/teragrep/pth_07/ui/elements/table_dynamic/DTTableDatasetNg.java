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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.AJAXRequest;
import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.Order;
import com.teragrep.pth_07.ui.elements.AbstractUserInterfaceElement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.interpreter.InterpreterContext;

import javax.json.*;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
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
    private final AngularObject<String> AJAXResponseAngularObject;

    private List<String> datasetAsJSON = null;
    private String datasetAsJSONSchema = "";
    private String datasetAsJSONFormattedSchema = "";

    private final Gson gson;

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
                getInterpreterContext().getParagraphId(), // tästä saa paragraphID:n
                true
        );

        AJAXRequestAngularObject.addWatcher(new AJAXRequestWatcher(interpreterContext, this));


        AJAXResponseAngularObject = getInterpreterContext().getAngularObjectRegistry().add(
                "AJAXResponse_"+getInterpreterContext().getParagraphId(),
                "{}",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );

        AJAXResponseAngularObject.addWatcher(new AJAXResponseWatcher(interpreterContext));
        this.gson = new Gson();
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

    void handeAJAXRequest(AJAXRequest ajaxRequest) {
        try {
            lock.lock();

            updatePage(ajaxRequest);
        }
        catch (JsonSyntaxException e) {
            LOGGER.error(e.toString());
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
            AJAXResponseAngularObject.emit();
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
                datasetAsJSONSchema = DTHeader.schemaToHeader(rowDataset.schema());
                datasetAsJSONFormattedSchema = DTHeader.schemaToJsonHeader(rowDataset.schema());
                datasetAsJSON = rowDataset.toJSON().collectAsList();
                //rowDataset.persist().limit(25); //nice

                AJAXRequest ajaxRequest = gson.fromJson((String) AJAXRequestAngularObject.get(), AJAXRequest.class);
                updatePage(ajaxRequest);
                // will scream at UI a lot, make nicer later
            }

        } catch (ParserConfigurationException | TransformerException e) {
            LOGGER.error(e.toString());
        } finally {
            lock.unlock();
        }
    }

    private void updatePage(AJAXRequest ajaxRequest){
        if (datasetAsJSON == null) {
            LOGGER.warn("attempting to draw empty dataset");
            return;
        }

        int currentAJAXID = 1;
        int currentAJAXStart = 0;
        String currentSearchString = "";
        List<Order> currentOrder = null;

        //ATOMIC INT AJAXLength & AJAXStart
        if (ajaxRequest != null) {
            //currentAJAXID = ajaxRequest.getDraw(); // We don't want to assign a draw value from the frontend, so we ignore it here
            currentAJAXStart = ajaxRequest.getStart();
            currentAJAXLength = ajaxRequest.getLength();
            currentSearchString = ajaxRequest.getSearch().getValue();
            currentOrder = ajaxRequest.getOrder();
        }

        // TODO these all decode the JSON, it refactor therefore to decode only once
        // searching
        List<String> searchedList = DTSearch.search(datasetAsJSON, currentSearchString);

        // TODO ordering
        //List<String> orderedlist = DTOrder.order(searchedList, currentOrder);
        List<String> orderedlist = searchedList;

        // pagination
        List<String> paginatedList = DTPagination.paginate(orderedlist, currentAJAXLength, currentAJAXStart);

        // ui formatting
        JsonArray formated = dataStreamParser(paginatedList);

        JsonObject headers;
        // header formatting
        if(datasetAsJSONFormattedSchema != ""){
            headers = Json.createReader(new StringReader(datasetAsJSONFormattedSchema)).readObject();
        }
        else {
            headers = Json.createObjectBuilder().build();
        }

        JsonObject response = DTNetResponse(formated, headers, currentAJAXID, orderedlist.size());
        AJAXResponseAngularObject.set(response.toString(), true);
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

    // Added headers to this object, should be contained in the AJAXResponse that UI receives.
    static JsonObject DTNetResponse(JsonArray data, JsonObject datasetAsJSONSchema, int ID, int length){
        try{
            JsonObjectBuilder builder = Json.createObjectBuilder();
            builder.add("headers",datasetAsJSONSchema);
            builder.add("data", data);
            builder.add("ID", ID);
            builder.add("datalength", length);
            return builder.build();
        }catch(JsonException|IllegalStateException e){
            LOGGER.error(e.toString());
            return(Json.createObjectBuilder().build());
        }
    }

}
