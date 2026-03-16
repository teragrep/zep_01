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
import com.teragrep.zep_01.interpreter.thrift.Options;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class UPlotFormat{

    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);
    /**
     * Formats a given Dataset to expected format for uPlot visualization library.
     */
    public UPlotFormat(){
    }

    /**
     * No-op. UPlot does not need to react to changes in dataset
     * @param dataset The updated dataset
     * @return This UPlotFormat
     */
    public UPlotFormat withDataset(final Dataset<Row> dataset){
        return this;
    }

    /**
     * Format a given Dataset into uPlot format using the parameters in the given Options object
     * @param dataset Dataset to format
     * @param options Thrift union object that must contain a UPlotOptions object.
     * @return JsonObject formatted to style expected by uPlot visualization library.
     * @throws InterpreterException Thrown when Dataset or Options given are invalid.
     */
    public JsonObject format(final Dataset<Row> dataset, final UPlotOptions options) throws InterpreterException{
        if(dataset.isEmpty()){
            throw new InterpreterException("Cannot format an empty Dataset!");
        }
        final StructType schema = dataset.schema();
        boolean aggsUsed = false;
        final List<String> groupByColumnNames = new ArrayList<>();
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
        }
        final Dataset<Row> concatenatedDataset = concatenateColumns(dataset,groupByColumnNames,concatenatedGroupByColumnName.toString());
        final List<Row> collectedData = concatenatedDataset.collectAsList();


        final JsonArray series = series(concatenatedDataset.schema());
        final JsonArray labels = labels(collectedData, aggsUsed);

        final JsonArray xAxis = xAxis(collectedData, aggsUsed);
        final List<JsonArray> yAxisArray = yAxis(collectedData);

        final JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        dataBuilder.add(xAxis);
        for (final JsonArray array:yAxisArray) {
            dataBuilder.add(array);
        }
        final JsonArray data = dataBuilder.build();

        final JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",data)
                .add("options",Json.createObjectBuilder()
                        .add("labels",labels)
                        .add("series",series)
                        .add("graphType",options.getGraphType()))
                .add("isAggregated",aggsUsed)
                .add("type", InterpreterResult.Type.UPLOT.label);
        final JsonObject json = builder.build();
        return json;
    }

    /**
     * Builds a JsonArray representing the indexes of the X-Axis values in uPlot
     * @param rows List of Rows in the Dataset to be formatted.
     * @param aggsUsed Whether aggregations were used in the Dataset to be formatted.
     * @return A JsonArray containing integers which can be mapped to group by labels to draw the X-Axis in uPlot. If no group by clause was used in the Dataset, an empty array is returned.
     */
    private JsonArray xAxis(final List<Row> rows, final boolean aggsUsed){
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        if(aggsUsed){
            for (int i = 0; i < rows.size(); i++) {
                builder.add(i);
            }
        }
        return builder.build();
    }

    /**
     * Builds a list of JsonArrays representing the Y-Axis in uPlot
     * @param rows List of Rows in the Dataset to be formatted. Rows must contain only numerical data (or stringified numerical data). Any columns containing metadata boolean "dpl-internal_isGroupByColumn" will be ignored.
     * @return A list of JsonArrays, each representing columnar data of a single series.
     * @throws InterpreterException Thrown when receiving non-numerical data, which is invalid for uPlot.
     */
    private List<JsonArray> yAxis(final List<Row> rows) throws InterpreterException {
        final List<JsonArray> yAxis = new ArrayList<>();
        final StructType schema = rows.get(0).schema();
            for (int i = 0; i < schema.fields().length; i++) {
                final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                final StructField field = schema.fields()[i];
                if(!field.metadata().contains("dpl_internal_isGroupByColumn")){
                    for (final Row row:rows) {
                        final DataType type = field.dataType();
                        final Object value = row.get(i);
                        if(type.equals(DataTypes.StringType)){
                            // Some data from DPL may come as numerical data, but stringified. If string data is encountered, try to parse into Double before throwing an Exception.
                            try{
                                final Double doubleValue = Double.parseDouble((String)value);
                                arrayBuilder.add(doubleValue);
                            }
                            catch (final NumberFormatException exception){
                                throw new InterpreterException("uPlot format only supports numerical data, but encountered unparseable string in column "+field.name()+" !");
                            }
                        }
                        else if (type.equals(DataTypes.LongType)){
                            final Long longValue = (Long) value;
                            arrayBuilder.add(longValue);
                        }
                        else if (type.equals(DataTypes.IntegerType)){
                            final Integer intValue = (Integer) value;
                            arrayBuilder.add(intValue);
                        }
                        else if (type.equals(DataTypes.DoubleType)){
                            final Double doubleValue = (Double) value;
                            arrayBuilder.add(doubleValue);
                        }
                        else if (type.equals(DataTypes.FloatType)){
                            final Float floatValue = (Float) value;
                            arrayBuilder.add(floatValue);
                        }
                        else if (type.equals(DataTypes.ShortType)){
                            final Short shortValue = (Short) value;
                            arrayBuilder.add(shortValue);
                        }
                        else {
                            throw new InterpreterException("uPlot format only supports numerical data, but encountered "+type.typeName()+" in column "+ field.name() +"!");
                        }
                    }
                    final JsonArray array = arrayBuilder.build();
                    yAxis.add(array);
                }
        }
        return yAxis;
    }

    /**
     * Builds a JsonArray containing the values within group by columns (such as timestamps), mapped to the X-Axis in uPlot.
     * @param rows List of Rows in the dataset to be formatted. Any column not containing metadata boolean "dpl-internal_isGroupByColumn" will be ignored.
     * @param aggsUsed Whether aggregations were used in the Dataset to be formatted.
     * @return JsonArray containing every value of every group by column used in the Dataset.
     */
    private JsonArray labels(final List<Row> rows, final boolean aggsUsed) {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        if(!rows.isEmpty() && aggsUsed){
            final StructType schema = rows.get(0).schema();
            for (final Row row:rows) {
                for (final StructField field:schema.fields()) {
                    if(field.metadata().contains("dpl_internal_isGroupByColumn")){
                        builder.add(row.get(row.fieldIndex(field.name())).toString());
                    }
                }
            }
        }
        return builder.build();
    }

    /**
     * Builds a JsonArray containing the names of all series that were not used in a group by clause.
     * @param schema Schema of the Dataset to be formatted
     * @return JsonArray containing the column names of all columns that do not contain metadata boolean "dpl_internal_isGroupByColumn"
     */
    private JsonArray series(final StructType schema){
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        for (final StructField field:schema.fields()) {
            if(! field.metadata().contains("dpl_internal_isGroupByColumn")){
                builder.add(field.name());
            }
        }
        return builder.build();
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
