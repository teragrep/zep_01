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

import jakarta.json.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * Responsible for formatting data received from BatchHandler into two different formats:
 * Formatting for Datatables so that it can be used as-is on the UI.
 * Formatting for InterpreterOutput so that it has the proper type.
 * Supports paginating and searching of the dataset based on user-supplied parameters.
 */

public final class DTTableDatasetNg implements DTTableDataset {
    // FIXME Exceptions should cause interpreter to stop
    private static final Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNg.class);
    private final StructType schema;
    private final List<String> dataset;
    private final int defaultLength = 50;

    public DTTableDatasetNg(final StructType schema, List<String> dataset){
        this.schema = schema;
        this.dataset = dataset;
    }

    @Override
    public String interpreterOutputFormat(int drawCount){
        return interpreterOutputFormat(drawCount, 0, defaultLength,"");
    }

    // When the dataset is passed through InterpreterOutput.write(), the data must be prepended with a type indicator (%jsontable in this case).
    @Override
    public String interpreterOutputFormat(int drawCount, int start, int length, String searchString) {
        JsonObject datasetAsJson = searchAndPaginate(drawCount, start,length,searchString);
        String formattedDataset = "%jsontable\n" +
                datasetAsJson.toString();
        return formattedDataset;
    }

    @Override
    public JsonObject datatablesFormat(int drawCount){
        return searchAndPaginate(drawCount, 0, defaultLength, "");
    }
    @Override
    public JsonObject datatablesFormat(int drawCount, int start, int length, String searchString){
        return searchAndPaginate(drawCount, start, length, searchString);
    }

    private JsonObject searchAndPaginate(int drawCount, int start, int length, String searchString) {
        // Data transformations based on user provided values
        List<String> searchedList = new DTSearch(searchString).apply(dataset);
        List<String> paginatedList = new DTPagination(start, length).apply(searchedList);

        // Metadata for UI
        int recordsTotal = dataset.size();
        int recordsFiltered = searchedList.size();

        // Build a response object
        JsonObjectBuilder builder = Json.createObjectBuilder();
        JsonArray headers = new DTHeader(schema).json();
        builder.add("headers",headers);
        JsonArray data = new DTData(paginatedList).json();
        builder.add("data", data);
        builder.add("draw", drawCount);
        builder.add("recordsTotal", recordsTotal);
        builder.add("recordsFiltered", recordsFiltered);
        JsonObject jsonResponse = builder.build();
        return jsonResponse;
    }

    @Override
    public List<String> dataset(){
        return dataset;
    }
    @Override
    public StructType schema(){
        return schema;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
