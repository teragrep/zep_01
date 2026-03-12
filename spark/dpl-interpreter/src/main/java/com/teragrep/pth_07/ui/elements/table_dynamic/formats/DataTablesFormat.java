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
package com.teragrep.pth_07.ui.elements.table_dynamic.formats;

import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Formats a given Dataset to expected format for DataTables visualization library.
 * Keeps an internal "draw" counter, which increments by 1 every time a new formatting request is received using the same Schema.
 * "draw" counter resets when a Dataset with a new Schema is encountered.
 * Keeps the rows of a Dataset in a cache to avoid unnecessary calls to Dataset.collectAsList() when performing for example pagination requests.
 * Cache is updated when a new Dataset is received
 */
public class DataTablesFormat{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTablesFormat.class);
    private final Dataset<Row> previousDataset;
    private final List<String> cachedRows;
    private final int draw;

    public DataTablesFormat(Dataset<Row> previousDataset, List<String> cachedRows, int draw){
        this.previousDataset = previousDataset;
        this.cachedRows = cachedRows;
        this.draw = draw;
    }

    /**
     * Create a new instance of DataTablesFormat with an updated Dataset. This function calculates any required updates to draw, and caches the rows of the Dataset if needed.
     * Caching is done to avoid repeated calls to Dataset.collectAsList() when formatting.
     * @param dataset The updated Dataset
     * @return A new instance fo DataTablesFormat, containing an updated draw value and cache of rows based on the given dataset.
     */
    public DataTablesFormat withDataset(Dataset<Row> dataset) {
        final int updatedDraw;
        final List<String> updatedCache;
        if(previousDataset.schema().equals(dataset.schema())){
            updatedDraw = draw +1;
        }
        else {
            updatedDraw = 1;
        }
        if(this.previousDataset.equals(dataset)){
            updatedCache = cachedRows;
        }
        else {
            updatedCache = dataset.toJSON().collectAsList();
        }
        return new DataTablesFormat(dataset, updatedCache, updatedDraw);
    }

    /**
     * Format the current Dataset into DataTables format using the parameters in the given Options object.
     * @param options Thrift union object that must contain a DataTablesOptions object.
     * @return JsonObject formatted to style expected by DataTables visualization library.
     */
    public JsonObject format(DataTablesOptions options){
        // headers
        JsonArrayBuilder headersBuilder = Json.createArrayBuilder();
        for (StructField header: this.previousDataset.schema().fields()) {
            headersBuilder.add(header.name());
        }
        JsonArray headers = headersBuilder.build();

        // search
        List<String> searchedRows = search(cachedRows, options.getSearch().getValue());

        // paginate
        List<String> paginatedRows = paginate(searchedRows, options.getStart(), options.getLength());

        // json
        JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        for (String jsonRow:paginatedRows) {
            dataBuilder.add(Json.createReader(new StringReader(jsonRow)).readObject());
        }
        JsonArray data = dataBuilder.build();
        long recordsTotal = cachedRows.size();
        long recordsFiltered = searchedRows.size();
        boolean isAggregated = isAggregated(this.previousDataset.schema());

        int draw = Math.max(this.draw,options.getDraw());

        JsonObject json = Json.createObjectBuilder()
                .add("data",Json.createObjectBuilder()
                        .add("headers",headers)
                        .add("data", data)
                        .add("draw", draw)
                        .add("recordsTotal", recordsTotal)
                        .add("recordsFiltered", recordsFiltered)
                        .build())
                .add("isAggregated",isAggregated)
                .add("type",InterpreterResult.Type.DATATABLES.label)
                .build();
        return json;
    }
    private boolean isAggregated(StructType schema) {
        for (StructField field:schema.fields()) {
            if(field.metadata().contains("dpl_internal_isGroupByColumn")){
                return true;
            }
        }
        return false;
    }

    public String type(){
        return InterpreterResult.Type.DATATABLES.label;
    }

    private List<String> search(List<String> rows, String searchString){
        List<String> searchedRows = new ArrayList<>();
        if (!"".equals(searchString)) {
            try {
                for (String row : rows) {
                    JsonReader reader = Json.createReader(new StringReader(row));
                    JsonObject line = reader.readObject();

                    // NOTE hard coded to _raw column
                    JsonString _raw = line.getJsonString("_raw");
                    if (_raw != null) {
                        String _rawString = _raw.getString();
                        if (_rawString != null) {
                            if (_rawString.contains(searchString)) {
                                // _raw matches, add whole row to result set
                                searchedRows.add(row);
                            }
                        }
                    }
                    reader.close();
                }
            } catch (JsonException | IllegalStateException e) {
                LOGGER.error(e.toString());
            }
        }
        else {
            searchedRows = rows;
        }
        return searchedRows;
    }

    private List<String> paginate(List<String> rows, int pageStart, int pageSize){
        // ranges must be greater than 0
        int fromIndex = Math.max(pageStart, 0);
        int toIndex = Math.max(fromIndex + pageSize, 0);

        // list must end at the maximum size
        if (toIndex > rows.size()) {
            toIndex = rows.size();
        }

        // list range must be positive
        if (fromIndex > toIndex) {
            fromIndex = toIndex;
        }

        List<String> paginatedRows = rows.subList(fromIndex, toIndex);
        return paginatedRows;
    }
}
