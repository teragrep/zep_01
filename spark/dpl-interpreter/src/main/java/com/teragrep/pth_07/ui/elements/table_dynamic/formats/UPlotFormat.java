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
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class UPlotFormat{

    private final UPlotData data;
    private final UPlotMetadata metadata;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);
    /**
     * Formats a given Dataset to expected format for uPlot visualization library.
     */
    public UPlotFormat(){
        this(new UPlotData(new ArrayList<>(),false), new UPlotMetadata(new StructType(),new ArrayList<>(),"line",false));
    }

    public UPlotFormat(UPlotData data, UPlotMetadata metadata){
        this.data = data;
        this.metadata = metadata;
    }


    /**
     * Create a new instance of UPlotFormat with an updated Dataset. This function calculates any required transformations UPlot format might need for the dataset and caches a UPlotData and UPlotMetadata objects for later use in formatting.
     * Caching is done to avoid repeated calls to Dataset.collectAsList() when using format() method for switching between graph types when the underlying dataset has not changed.
     * @param newDataset The updated Dataset
     * @return A new instance of UPlotFormat, containing an updated UPlotData and UPlotMetadata objects ready for formatting.
     */
    public UPlotFormat withDataset(final Dataset<Row> newDataset) {
        final StructType schema = newDataset.schema();
        boolean aggsUsed = false;
        final List<String> groupByColumnNames = new ArrayList<>();
        final List<String> valueColumnNames = new ArrayList<>();
        String delimiter = "";
        final StringBuilder concatenatedGroupByColumnName = new StringBuilder();
        for (final StructField field:schema.fields()) {
            if (field.metadata().contains("dpl_internal_isGroupByColumn")) {
                aggsUsed = true;
                concatenatedGroupByColumnName.append(delimiter);
                delimiter = "|";
                concatenatedGroupByColumnName.append(field.name());
                groupByColumnNames.add(field.name());
            }
            else {
                valueColumnNames.add(field.name());
            }
        }
        final Dataset<Row> transformedDataset;
        // Timechart case
        if(groupByColumnNames.contains("_time")){
            if(groupByColumnNames.size() == 1){
                transformedDataset = newDataset;
            }
            else {
                List<String> timechartGroupByColumnNames = new ArrayList<>(groupByColumnNames);
                timechartGroupByColumnNames.remove("_time");

                List<Column> columns = new ArrayList<>();
                for (int i = 0; i < valueColumnNames.size(); i++) {
                    columns.add(org.apache.spark.sql.functions.first(valueColumnNames.get(i)).alias(valueColumnNames.get(i)));
                }
                Column first = columns.get(0);
                columns.remove(0);
                Column[] rest = columns.toArray(new Column[0]);

                transformedDataset = newDataset.groupBy("_time")
                        .pivot(timechartGroupByColumnNames.get(0))
                        .agg(first,rest);
            }
        }

        // Other cases
        else {
            transformedDataset = concatenateColumns(newDataset,groupByColumnNames,concatenatedGroupByColumnName.toString());
        }
        List<Row> collectedData = transformedDataset.collectAsList();
        StructType transformedSchema = transformedDataset.schema();
        return new UPlotFormat(new UPlotData(collectedData,aggsUsed),new UPlotMetadata(transformedSchema,collectedData,"line",aggsUsed));
    }

    public JsonObject format(UPlotOptions options) throws InterpreterException{
        final UPlotMetadata updatedMetadata = metadata.withOptions(options);
        final JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",data.asJson())
                .add("options",updatedMetadata.asJson())
                .add("isAggregated",updatedMetadata.isAggregated())
                .add("type", InterpreterResult.Type.UPLOT.label);
        final JsonObject json = builder.build();
        return json;
    }

    /**
     * Concatenates given columns in a Dataset to form a single column, adding a "." as a separator.
     * Used to combine labels to expected format when a dataset is using multiple "group by" values
     * @param dataset Dataset to modify
     * @param columnNamesToConcatenate List of column names identifying columns to concatenate
     * @param concatenatedColumnName A new Column name for the combined columns
     * @return Modified Dataset
     */
    private Dataset<Row> concatenateColumns(final Dataset<Row> dataset, final List<String> columnNamesToConcatenate, final String concatenatedColumnName){
        // If trying to concatenate less than two columns, we don't need to do any transformations to the data,
        if(columnNamesToConcatenate.size() < 2){
            return dataset;
        }
        // Get columns that were used in grouping of data. These will be concatenated to a new column and then dropped.
        final List<Column> groupByColumns = new ArrayList<>();
        for (final String columnName:columnNamesToConcatenate) {
            groupByColumns.add(dataset.col(columnName));
        }

        // Create a Dataset containing the concatenated groupBy column.
        final Dataset<Row> concatenatedDataset = dataset.withColumn(concatenatedColumnName,functions.concat_ws(".",groupByColumns.toArray(new Column[]{})))
                .drop(columnNamesToConcatenate.toArray(new String[0]))
                .withMetadata(concatenatedColumnName,new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());
        return concatenatedDataset;
    }

    public String type(){
        return InterpreterResult.Type.UPLOT.label;
    }
}
