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

import java.util.Arrays;
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

    public DataTablesFormat(final UIOption option, final Dataset<Row> dataset){
        this.option = option;
        this.dataset = dataset;
    }

    /**
     * Format the current Dataset into DataTables format using the parameters in the given Options object.
     * This will paginate the cached rows based on Options parameters.
     * Operates on the cached rows of this DataTablesFormat object. Repeated calls paginates the same data with given parameter. If the cache needs to be updated, use .withDataset() to create a new DataTablesFormat object.
     * @return JsonObject formatted to the style expected by DataTables visualization library, with requested pagination performed.
     */
    @Override
    public JsonObject asJson(){
        final JsonObject optionJson = option.asJson().getJsonObject("requestOptions");
        final List<String> rows = dataset.toJSON().collectAsList();
        final int pageStart = optionJson.getInt("start");
        final int pageLength = optionJson.getInt("length");

        final boolean isAggregated = isAggregated(dataset.schema());
        final int draw = optionJson.getInt("draw");

        final DataTablesMetadata dataTablesMetadata = new DataTablesMetadata(dataset.schema());
        final DataTablesData dataTablesData = new DataTablesData(rows,draw,pageStart,pageLength,"");

        final JsonObjectBuilder json = Json.createObjectBuilder()
                .add("data",dataTablesData.asJson())
                .add("options",dataTablesMetadata.asJson())
                .add("isAggregated",isAggregated)
                .add("type", InterpreterResult.Type.DATATABLES.label);

        return json.build();
    }
    private boolean isAggregated(final StructType schema) {
        return Arrays.stream(schema.fields()).anyMatch(field -> field.metadata().contains("dpl_internal_isGroupByColumn"));
    }

    public InterpreterResult.Type type(){
        return InterpreterResult.Type.DATATABLES;
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final DataTablesFormat format = (DataTablesFormat) o;
            equals = Objects.equals(option, format.option) && Objects.equals(dataset, format.dataset);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(option, dataset);
    }
}
