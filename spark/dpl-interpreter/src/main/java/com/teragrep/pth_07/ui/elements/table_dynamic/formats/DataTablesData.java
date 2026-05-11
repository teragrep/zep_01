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

import jakarta.json.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class DataTablesData {

    private final List<String> collectedData;
    private final long draw;
    private final String searchString;
    private final int pageStart;
    private final int pageLength;

    public DataTablesData(final List<String> collectedData, final long draw, final int pageStart, final int pageLength, final String searchString){
        this.collectedData = collectedData;
        this.draw = draw;
        this.searchString = searchString;
        this.pageStart = pageStart;
        this.pageLength = pageLength;
    }

    private List<String> paginate(final int pageStart, int pageSize){
        if(pageSize == 0){
            pageSize = collectedData.size();
        }
        // ranges must be greater than 0
        int fromIndex = Math.max(pageStart, 0);
        int toIndex = Math.max(fromIndex + pageSize, 0);

        // list must end at the maximum size
        if (toIndex > collectedData.size()) {
            toIndex = collectedData.size();
        }

        // list range must be positive
        if (fromIndex > toIndex) {
            fromIndex = toIndex;
        }

        final List<String> paginatedRows = collectedData.subList(fromIndex, toIndex);
        return paginatedRows;
    }


    private JsonArray data(){
        List<String> paginatedRows = paginate(pageStart, pageLength);
        final JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        for (final String jsonRow:paginatedRows) {
            dataBuilder.add(Json.createReader(new StringReader(jsonRow)).readObject());
        }
        return dataBuilder.build();
    }

    private long draw(){
        return draw;
    }

    /**
     * @return long number equal to the count of records in the dataset
     */
    private long recordsTotal(){
        return collectedData.size();
    }

    /**
     * @return long number equal to the count of records matching the "searchString" in the dataset
     */
    // Searching is not yet implemented, so will always return the size of the entire dataset.
    private long recordsFiltered(){
        return collectedData.size();
    }

    public JsonValue asJson() {
        final JsonObject json = Json.createObjectBuilder()
                        .add("data", data())
                        .add("draw", draw())
                        .add("recordsTotal", recordsTotal())
                        .add("recordsFiltered", recordsFiltered())
                .build();
        return json;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTablesData that = (DataTablesData) o;
        return draw == that.draw && pageStart == that.pageStart && pageLength == that.pageLength && Objects.equals(collectedData, that.collectedData) && Objects.equals(searchString, that.searchString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectedData, draw, searchString, pageStart, pageLength);
    }
}
