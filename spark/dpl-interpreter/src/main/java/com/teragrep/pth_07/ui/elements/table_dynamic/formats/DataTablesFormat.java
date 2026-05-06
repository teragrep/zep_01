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
import java.util.Objects;

/**
 * Formats a given Dataset to expected format for DataTables visualization library.
 * Keeps an internal "draw" counter, which increments by 1 every time a new formatting request is received using the same Schema.
 * "draw" counter resets when a Dataset with a new Schema is encountered.
 * Keeps the rows of a Dataset in a cache to avoid unnecessary calls to Dataset.collectAsList() when performing for example pagination requests.
 * Cache is updated when a new Dataset is received
 */
public final class DataTablesFormat implements RenderFormat{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTablesFormat.class);

    private final UIOption option;
    private final Dataset<Row> dataset;

    public DataTablesFormat(UIOption option, Dataset<Row> dataset){
        this.option = option;
        this.dataset = dataset;
    }

    /**
     * Format the current Dataset into DataTables format using the parameters in the given Options object.
     * This will paginate the cached rows based on Options parameters.
     * Operates on the cached rows of this DataTablesFormat object. Repeated calls paginates the same data with given parameter. If the cache needs to be updated, use .withDataset() to create a new DataTablesFormat object.
     * @return JsonObject formatted to the style expected by DataTables visualization library, with requested pagination performed.
     */
    public JsonObject format(){
        JsonObject optionJson = option.toJson().getJsonObject("requestOptions");
        // headers
        final JsonArrayBuilder headersBuilder = Json.createArrayBuilder();
        for (final StructField header: dataset.schema().fields()) {
            headersBuilder.add(header.name());
        }
        final JsonArray headers = headersBuilder.build();

        // search, not implemented yet
        final List<String> rows = dataset.toJSON().collectAsList();
        final List<String> searchedRows = search(rows, "");

        // paginate
        final List<String> paginatedRows = paginate(searchedRows, optionJson.getInt("start"), optionJson.getInt("length"));

        // json
        final JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        for (final String jsonRow:paginatedRows) {
            dataBuilder.add(Json.createReader(new StringReader(jsonRow)).readObject());
        }
        final JsonArray data = dataBuilder.build();
        final long recordsTotal = rows.size();
        final long recordsFiltered = searchedRows.size();
        final boolean isAggregated = isAggregated(dataset.schema());
        final int draw = optionJson.getInt("draw");

        final JsonObject json = Json.createObjectBuilder()
                .add("data",Json.createObjectBuilder()
                        .add("data", data)
                        .add("draw", draw)
                        .add("recordsTotal", recordsTotal)
                        .add("recordsFiltered", recordsFiltered)
                        .build())
                .add("options",Json.createObjectBuilder()
                        .add("headers",headers)
                        .build())
                .add("isAggregated",isAggregated)
                .add("type",InterpreterResult.Type.DATATABLES.label)
                .build();
        return json;
    }
    private boolean isAggregated(final StructType schema) {
        for (final StructField field:schema.fields()) {
            if(field.metadata().contains("dpl_internal_isGroupByColumn")){
                return true;
            }
        }
        return false;
    }

    public InterpreterResult.Type type(){
        return InterpreterResult.Type.DATATABLES;
    }

    private List<String> search(final List<String> rows, final String searchString){
        List<String> searchedRows = new ArrayList<>();
        if (!searchString.isEmpty()) {
            for (final String row : rows) {
                try (final JsonReader reader = Json.createReader(new StringReader(row))){
                    final JsonObject line = reader.readObject();
                    // NOTE hard coded to _raw column
                    final JsonString _raw = line.getJsonString("_raw");
                    if (_raw != null) {
                        final String _rawString = _raw.getString();
                        if (_rawString != null) {
                            if (_rawString.contains(searchString)) {
                                // _raw matches, add whole row to result set
                                searchedRows.add(row);
                            }
                        }
                    }
                }
                catch (JsonException e){
                    LOGGER.error(e.toString());
                }
            }
        }
        else {
            searchedRows = rows;
        }
        return searchedRows;
    }

    private List<String> paginate(final List<String> rows, final int pageStart, int pageSize){
        if(pageSize == 0){
            pageSize = rows.size();
        }
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

        final List<String> paginatedRows = rows.subList(fromIndex, toIndex);
        return paginatedRows;
    }

    @Override
    public JsonObject toJson() {
        return format();
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTablesFormat format = (DataTablesFormat) o;
        return Objects.equals(option, format.option) && Objects.equals(dataset, format.dataset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(option, dataset);
    }
}
