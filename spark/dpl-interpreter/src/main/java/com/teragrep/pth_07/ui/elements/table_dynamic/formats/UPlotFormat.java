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
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class UPlotFormat implements RenderFormat{

    private final UIOption option;
    private final Dataset<Row> dataset;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);
    /**
     * Formats a given Dataset to expected format for uPlot visualization library.
     */

    public UPlotFormat(final UIOption option, final Dataset<Row> rowDataset){
        this.option = option;
        this.dataset = rowDataset;
    }

    @Override
    public JsonObject asJson(){
        final JsonObject requestOptions = option.asJson().getJsonObject("requestOptions");
        final StructType schema = dataset.schema();
        final List<String> groupByColumnNames = new ArrayList<>();
        final List<String> valueColumnNames = new ArrayList<>();
        for (final StructField field:schema.fields()) {
            // We detect grouping columns by metadata instead of LogicalPlan because StreamingQueries created in batches have their LogicalPlans overwritten.
            if (field.metadata().contains("dpl_internal_isGroupByColumn")) {
                groupByColumnNames.add(field.name());
            }
            else {
                valueColumnNames.add(field.name());
            }
        }
        final boolean aggsUsed = !groupByColumnNames.isEmpty();
        // Datasets grouped by _time column (such as those created using timechart command) require different formatting than datasets without such grouping.

        final List<String> xAxisColumnNames = new ArrayList<>();
        if(groupByColumnNames.size() > 0){
            xAxisColumnNames.add(groupByColumnNames.get(0));
            groupByColumnNames.remove(0);
        }

        final UPlotDatasetTransformation transformation = new UPlotDatasetTransformation(dataset, xAxisColumnNames, groupByColumnNames, valueColumnNames);
        final Dataset<Row> transformedDataset = transformation.apply();

        final List<Row> rows = transformedDataset.collectAsList();
        final String graphType = requestOptions.getString("graphType");
        final String xAxisLabel = String.join(".", xAxisColumnNames);
        final UPlotMetadata uPlotMetadata = new UPlotMetadata(transformedDataset.schema(),rows,xAxisLabel,graphType,aggsUsed);
        final UPlotData uplotData = new UPlotData(rows,aggsUsed);

        final JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",uplotData.asJson())
                .add("options",uPlotMetadata.asJson())
                .add("isAggregated",uPlotMetadata.isAggregated())
                .add("type", InterpreterResult.Type.UPLOT.label);
        return builder.build();
    }

    public InterpreterResult.Type type(){
        return InterpreterResult.Type.UPLOT;
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
            final UPlotFormat format = (UPlotFormat) o;
            equals = Objects.equals(option, format.option) && Objects.equals(dataset, format.dataset);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(option, dataset);
    }
}
