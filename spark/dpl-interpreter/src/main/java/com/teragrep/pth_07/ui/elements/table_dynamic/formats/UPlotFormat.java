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

public class UPlotFormat implements  DatasetFormat{

    private final UPlotOptions options;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);

    public UPlotFormat(final UPlotOptions options){
        this.options = options;
    }

    public JsonObject format(Dataset<Row> dataset) throws InterpreterException{
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
        final JsonArray yAxis = yAxis(collectedData);

        final JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(xAxis)
                        .add(yAxis))
                .add("options",Json.createObjectBuilder()
                        .add("labels",labels)
                        .add("series",series)
                        .add("graphType",options.getGraphType()))
                .add("isAggregated",aggsUsed)
                .add("type", InterpreterResult.Type.UPLOT.label);
        final JsonObject json = builder.build();
        return json;
    }

    private JsonArray xAxis(final List<Row> rows, final boolean aggsUsed){
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        if(aggsUsed){
            for (int i = 0; i < rows.size(); i++) {
                builder.add(i);
            }
        }
        return builder.build();
    }
    private JsonArray yAxis(final List<Row> rows) throws InterpreterException {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        final StructType schema = rows.get(0).schema();
            for (int i = 0; i < schema.fields().length; i++) {
                final StructField field = schema.fields()[i];
                if(!field.metadata().contains("dpl_internal_isGroupByColumn")){
                    final JsonArrayBuilder subArrayBuilder = Json.createArrayBuilder();
                    for (final Row row:rows) {
                        final DataType type = field.dataType();
                        final Object value = row.get(i);
                        if(type.equals(DataTypes.StringType)){
                            // If string data is encountered, try to parse into Double
                            try{
                                final Double doubleValue = Double.parseDouble((String)value);
                                subArrayBuilder.add(doubleValue);
                            }
                            catch (final NumberFormatException exception){
                                throw new InterpreterException("uPlot format only supports numerical data, but encountered unparseable string in column "+field.name()+" !");
                            }
                        }
                        else if (type.equals(DataTypes.LongType)){
                            final Long longValue = (Long) value;
                            subArrayBuilder.add(longValue);
                        }
                        else if (type.equals(DataTypes.IntegerType)){
                            final Integer intValue = (Integer) value;
                            subArrayBuilder.add(intValue);
                        }
                        else if (type.equals(DataTypes.DoubleType)){
                            final Double doubleValue = (Double) value;
                            subArrayBuilder.add(doubleValue);
                        }
                        else if (type.equals(DataTypes.FloatType)){
                            final Float floatValue = (Float) value;
                            subArrayBuilder.add(floatValue);
                        }
                        else if (type.equals(DataTypes.ShortType)){
                            final Short shortValue = (Short) value;
                            subArrayBuilder.add(shortValue);
                        }
                        else {
                            throw new InterpreterException("uPlot format only supports numerical data, but encountered "+type.typeName()+" in column "+ field.name() +"!");
                        }
                    }
                    builder.add(subArrayBuilder.build());
                }
        }
        return builder.build();
    }

    private JsonArray labels(final List<Row> rows, final boolean aggsUsed) {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        final StructType schema = rows.get(0).schema();
        if(aggsUsed){
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
    private JsonArray series(final StructType schema){
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        for (final StructField field:schema.fields()) {
            if(! field.metadata().contains("dpl_internal_isGroupByColumn")){
                builder.add(field.name());
            }
        }
        return builder.build();
    }


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

    @Override
    public String type(){
        return InterpreterResult.Type.UPLOT.label;
    }
}
