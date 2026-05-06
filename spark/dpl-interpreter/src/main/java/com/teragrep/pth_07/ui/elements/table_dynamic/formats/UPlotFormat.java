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

    public UPlotFormat(UIOption option, Dataset<Row> rowDataset){
        this.dataset = rowDataset;
        this.option = option;
    }

    /**
     * Creates a appropriately transformed dataset when a dataset's X-axis should be a timescale. Dataset should contain a column named "_time"
     * This method pivots the dataset so that only "_time" is used as the X-axis, and combinations of unique values in other group by columns are combined into different series on the Y-axis.
     * @param dataset The dataset to transform
     * @param groupByColumnNames List of column names used in group by clauses
     * @param valueColumnNames List of column names used outside of group by clauses
     * @return A transformed dataset ready for formatting.
     */
    private Dataset<Row> timechartTransformation(final Dataset<Row> dataset, final List<String> groupByColumnNames, final List<String> valueColumnNames){
        final Dataset<Row> transformedDataset;
        if(groupByColumnNames.size() < 2){
            transformedDataset = dataset;
        }
        else {
            // Grouping columns as Column array for transformation
            final List<String> timechartGroupByColumnNames = new ArrayList<>(groupByColumnNames);
            timechartGroupByColumnNames.remove("_time");

            final List<Column> columns = new ArrayList<>();
            for (int i = 0; i < valueColumnNames.size(); i++) {
                columns.add(org.apache.spark.sql.functions.first(valueColumnNames.get(i)).alias(valueColumnNames.get(i)));
            }
            final Column first = columns.get(0);
            columns.remove(0);
            final Column[] rest = columns.toArray(new Column[0]);

            final List<Column> groupByColumns = new ArrayList<>();
            for (int i = 0; i < timechartGroupByColumnNames.size(); i++) {
                groupByColumns.add(org.apache.spark.sql.functions.col(timechartGroupByColumnNames.get(i)));
            }
            final Column[] groupByColumnArray = groupByColumns.toArray(new Column[0]);
            Dataset<Row> pivotedDataset = dataset
                    .withColumn("pivot", org.apache.spark.sql.functions.concat_ws(".",groupByColumnArray))
                    .groupBy("_time")
                    .pivot("pivot")
                    .agg(first,rest);

            // pivot() uses an underscore as the separator when it creates new columns, and it cannot be overridden. To use "." as separator we have to rename the columns.
            for (final StructField column : pivotedDataset.schema().fields()) {
                for (final String valueColumn : valueColumnNames){
                    if(column.name().endsWith("_"+valueColumn)){
                        final String existingName = column.name();
                        final int separatorIndex = existingName.indexOf("_"+valueColumn);
                        final StringBuilder newName = new StringBuilder(existingName);
                        newName.setCharAt(separatorIndex,'.');
                        pivotedDataset = pivotedDataset.withColumnRenamed(existingName, newName.toString());
                    }
                }
            }
            transformedDataset = pivotedDataset;
        }
        return transformedDataset;
    }

    /**
     * Creates a transformed dataset when a dataset's X-axis should consist of the combinations of all used grouping labels.
     * This method transforms the dataset so that
     * @param dataset Dataset to transform
     * @param groupByColumnNames List of names used in group by clauses
     * @return A new dataset ready for formatting.
     */

    private Dataset<Row> aggregationTransformation(final Dataset<Row> dataset, final List<String> groupByColumnNames){
        final Dataset<Row> transformedDataset;
        // If trying to concatenate less than two columns, we don't need to do any transformations to the data,
        if(groupByColumnNames.size() < 2){
            transformedDataset = dataset;
        }
        else {
            // Get columns that were used in grouping of data. These will be concatenated to a new column and then dropped.
            final List<Column> groupByColumns = new ArrayList<>();
            for (final String columnName:groupByColumnNames) {
                groupByColumns.add(dataset.col(columnName));
            }

            // Create a Dataset containing the concatenated groupBy column. Copy metadata as well since it's being used later when creating labels.
            transformedDataset = dataset.withColumn("label",functions.concat_ws(".",groupByColumns.toArray(new Column[]{})))
                    .drop(groupByColumnNames.toArray(new String[0]))
                    .withMetadata("label",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());
        }
        return transformedDataset;
    }


    public JsonObject format(){
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
        final Dataset<Row> transformedDataset;
        // Datasets grouped by _time column (such as those created using timechart command) require different formatting than datasets without such grouping.
        if(groupByColumnNames.contains("_time")){
            transformedDataset = timechartTransformation(dataset,groupByColumnNames,valueColumnNames);
        }
        else {
            transformedDataset = aggregationTransformation(dataset,groupByColumnNames);
        }

        List<Row> rows = transformedDataset.collectAsList();
        String graphType = option.toJson().getJsonObject("requestOptions").getString("graphType");
        final UPlotMetadata uPlotMetadata = new UPlotMetadata(dataset.schema(),rows,graphType,aggsUsed);
        UPlotData uplotData = new UPlotData(rows,aggsUsed);

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
        UPlotFormat format = (UPlotFormat) o;
        return Objects.equals(option, format.option) && Objects.equals(dataset, format.dataset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(option, dataset);
    }
}
