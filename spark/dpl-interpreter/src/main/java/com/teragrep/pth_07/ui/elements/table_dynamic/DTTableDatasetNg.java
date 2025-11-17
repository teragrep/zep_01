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
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.List;


// Encapsulates a Spark Dataset and outputs a String representation of it in the format expected by UI.
// Capable of searching and paginating the output when provided with a search string and/or pagination start index and length
public final class DTTableDatasetNg implements DTTableDataset {
    // FIXME Exceptions should cause interpreter to stop
    static Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNg.class);
    private final Dataset<Row> dataset;
    private int defaultLength = 50;

    public DTTableDatasetNg(final Dataset<Row> dataset){
        this.dataset = dataset;
    }


    @Override
    public String drawDataset(int drawCount){
        return drawDataset(0, defaultLength,"",drawCount);
    }

    // When the dataset is passed through InterpreterOutput.write(), the data must be prepended with a type indicator (%jsontable in this case).
    @Override
    public String drawDataset(int start, int length, String searchString, int drawCount) {
        JsonObject datasetAsJson = searchAndPaginate(drawCount, start,length,searchString);
        String formattedDataset = "%jsontable\n" +
                datasetAsJson.toString();
        return formattedDataset;
    }

    // Return a JsonObject representing the dataset with given search string and pagination information.
    @Override
    public JsonObject searchAndPaginate(int draw, int start, int length, String searchString) {
        List<String> datasetAsJson = dataset.toJSON().collectAsList();

        DTSearch dtSearch = new DTSearch(datasetAsJson);
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
        DTHeader schemaHeaders = new DTHeader(dataset.schema());
        final JsonArray schemaHeadersAsJSON = schemaHeaders.json();
        int recordsTotal = (int) dataset.count();
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

    // Return encapsulated dataset
    @Override
    public Dataset<Row> getDataset(){
        return dataset;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
