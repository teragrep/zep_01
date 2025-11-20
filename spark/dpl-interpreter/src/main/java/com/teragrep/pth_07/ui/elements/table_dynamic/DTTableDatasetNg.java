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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;


// Encapsulates a Spark Dataset and outputs a String representation of it in the format expected by UI.
// Capable of searching and paginating the output when provided with a search string and/or pagination start index and length
public final class DTTableDatasetNg implements DTTableDataset {
    // FIXME Exceptions should cause interpreter to stop
    static Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNg.class);
    private final CachedDataset cachedDataset;
    private int defaultLength = 50;

    public DTTableDatasetNg(final Dataset<Row> dataset){
        this(new CachedDataset(dataset));
    }

    public DTTableDatasetNg(final CachedDataset cachedDataset){
        this.cachedDataset = cachedDataset;
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
        // Data transformations
        List<String> datasetAsJson = cachedDataset.cache();
        List<String> searchedList = new DTSearch(searchString).apply(datasetAsJson);
        List<String> paginatedList = new DTPagination(start, length).apply(searchedList);

        // Metadata for UI
        int recordsTotal = (int) cachedDataset.dataset().count();
        int recordsFiltered = searchedList.size();

        return DTNetResponse(paginatedList, cachedDataset.dataset().schema(), draw, recordsTotal,recordsFiltered);
    }

    private JsonObject DTNetResponse(List<String> rowList, StructType schema, int draw, int recordsTotal, int recordsFiltered){
        try{
            JsonObjectBuilder builder = Json.createObjectBuilder();
            JsonArray headers = new DTHeader(schema).json();
            builder.add("headers",headers);
            JsonArray data = new DTData(rowList).json();
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
        return cachedDataset.dataset();
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
