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

import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public final class UPlotMetadata {
    private final StructType schema;
    private final List<Row> collectedData;
    private final String graphType;
    private final boolean isAggregated;

    public UPlotMetadata(final StructType schema, final List<Row> collectedData, final String graphType, final boolean isAggregated){
        this.schema = schema;
        this.collectedData = collectedData;
        this.graphType = graphType;
        this.isAggregated = isAggregated;
    }

    public UPlotMetadata withOptions(final UPlotOptions options){
        return new UPlotMetadata(schema,collectedData,options.getGraphType(),isAggregated);
    }

    /**
     * Builds a JsonArray containing the names of all series that were not used in a group by clause.
     * @return JsonArray containing the column names of all columns that do not contain metadata boolean "dpl_internal_isGroupByColumn"
     */
    private JsonArray series(){
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        for (final StructField field:schema.fields()) {
            if(! field.metadata().contains("dpl_internal_isGroupByColumn")){
                builder.add(field.name());
            }
        }
        return builder.build();
    }

    /**
     * Builds a JsonArray containing the values within group by columns (such as timestamps), mapped to the X-Axis in uPlot.
     * @return JsonArray containing every value of every group by column used in the Dataset.
     */
    private JsonArray labels() {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        if(!collectedData.isEmpty() && isAggregated){
            for (final Row row:collectedData) {
                for (final StructField field:schema.fields()) {
                    if(field.metadata().contains("dpl_internal_isGroupByColumn")){
                        builder.add(row.get(row.fieldIndex(field.name())).toString());
                    }
                }
            }
        }
        return builder.build();
    }

    public boolean isAggregated(){
        return isAggregated;
    }


    public JsonValue asJson() {
        return Json.createObjectBuilder()
                .add("labels",labels())
                .add("series",series())
                .add("graphType", graphType)
                .build();
    }
}
