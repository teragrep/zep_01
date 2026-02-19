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
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UPlotFormat implements  DatasetFormat{

    private final Dataset<Row> dataset;
    private final UPlotOptions options;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);

    public UPlotFormat(final Dataset<Row> dataset, final UPlotOptions options){
        this.dataset = dataset;
        this.options = options;
    }

    public JsonObject format() throws InterpreterException{
        if(dataset.isEmpty()){
            throw new InterpreterException("Cannot format an empty Dataset!");
        }
        // Get a list of column names that were used in aggregation
        final List<String> groupByLabels = new ArrayList<>();
        final LogicalPlan plan = dataset.queryExecution().logical();
        final boolean aggsUsed;
        if (plan instanceof Aggregate) {
            aggsUsed = true;
            final Aggregate aggPlan = (Aggregate) plan;
            final List<Expression> expressions = JavaConverters.seqAsJavaList((aggPlan.groupingExpressions().seq()));
            for (final Expression expression: expressions) {
                if(expression instanceof AttributeReference){
                    final AttributeReference attributeReference = (AttributeReference)expression;
                    groupByLabels.add(attributeReference.name());
                }
            }
        }
        else {
            aggsUsed = false;
        }

        // Get references to Column objects for each column used in aggregation
        final List<Column> groupByColumns = new ArrayList<>();
        for (final String columnName:groupByLabels) {
            groupByColumns.add(dataset.col(columnName));
        }

        // Turn both lists into Arrays
        final String[] groupByLabelsArray = groupByLabels.toArray(new String[0]);
        final Column[] groupByColumnArray = groupByColumns.toArray(new Column[0]);
        final Dataset<Row> concatenatedDataset;
        final StringBuilder concatenatedGroupByLabel = new StringBuilder();

        // If there are more than one label used in grouping data, the names must be concatenated into a single column. eg: "group1|group2|group3"
        if(groupByLabels.size() > 1){
            for (int i = 0; i < groupByLabels.size(); i++) {
                concatenatedGroupByLabel.append(groupByLabels.get(i));
                if(i < groupByLabels.size()-1){
                    concatenatedGroupByLabel.append("|");
                }
            }
            concatenatedDataset = dataset.withColumn(concatenatedGroupByLabel.toString(),functions.concat_ws("|",groupByColumnArray)).drop(groupByLabelsArray);
        }
        else if(groupByLabels.size() == 1){
            concatenatedGroupByLabel.append(groupByLabels.get(0));
            concatenatedDataset = dataset;
        }
        else {
            concatenatedDataset = dataset;
        }

        // Transpose the dataset containing the concatenated column created above.
        // As Spark does not support columnar data, this is accomplished by collecting all the data under each column into a list and generating a Dataset with a single row containing those lists as its data.
        final List<Column> columns = new ArrayList<Column>();
        for (final String columnName : concatenatedDataset.columns()) {
            columns.add(org.apache.spark.sql.functions.collect_list(columnName).as(columnName));
        }
        final Column[] columnArray = columns.toArray(new Column[0]); //We need to separate the first column from the others to satisfy the parameters of Dataset.agg(Column, Column...)
        final Column firstColumn = columnArray[0];
        final Column[] additionalColumns = Arrays.copyOfRange(columnArray,1,columnArray.length);
        final Dataset<Row> transposed = concatenatedDataset.agg(firstColumn,additionalColumns);

        // Finally, create a JSON object as expected by uPlot.
        final JsonArrayBuilder data0 = Json.createArrayBuilder();
        final JsonArrayBuilder data1 = Json.createArrayBuilder();
        final JsonArrayBuilder labels = Json.createArrayBuilder();
        final JsonArrayBuilder series = Json.createArrayBuilder();
        final String graphType = options.getGraphType();

        // Transposition collected all the data into a single row containing a number of lists.
        final Row resultRow = transposed.collectAsList().get(0);
        for (int i = 0; i < resultRow.schema().size(); i++) {
            final StructField schemaField = resultRow.schema().fields()[i];
            // Aggregated data labels should be added to options.labels, as well as their indexes in the first sub-array of the data object.
            if(schemaField.name().equals(concatenatedGroupByLabel.toString())){
                final List<Object> values = resultRow.getList(i);
                int valueIndex = 0;
                for (final Object value:values) {
                    data0.add(valueIndex); // Is data[0] necessary to have? aggregated data should never have duplicated labels for aggregation group names so you could just use options.labels for this information
                    labels.add(value.toString());
                    valueIndex++;
                }
            }
            // Standard data must be cast into a numerical type based on the Schema. uPlot does not support displaying non-numerical data at all.
            else {
                final List<Object> values = resultRow.getList(i);
                final JsonArrayBuilder subArray = Json.createArrayBuilder();
                final DataType elementType = ((ArrayType)(schemaField).dataType()).elementType();
                    if(elementType.equals(DataTypes.IntegerType)){
                        for (final Object value:values) {
                            subArray.add((int) value);
                        }
                    } else if (elementType.equals(DataTypes.LongType)) {
                        for (final Object value:values) {
                            subArray.add((Long) value);
                        }
                    }
                    else if (elementType.equals(DataTypes.ShortType)) {
                        for (final Object value:values) {
                            subArray.add((Short) value);
                        }
                    }
                    else if (elementType.equals(DataTypes.DoubleType)) {
                        for (final Object value:values) {
                            subArray.add((Double) value);
                        }
                    }
                    else if (elementType.equals(DataTypes.FloatType)) {
                        for (final Object value:values) {
                            subArray.add((Float) value);
                        }
                    }
                    else{
                        throw new InterpreterException("uPlot format only supports numerical data, tried to format data of type "+elementType+" in column "+ schemaField.name() +"!");
                    }
                data1.add(subArray.build());
                series.add(schemaField.name());
            }
        }
        final JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(data0)
                        .add(data1))
                .add("options",Json.createObjectBuilder()
                        .add("labels",labels)
                        .add("series",series)
                        .add("graphType",graphType))
                .add("isAggregated",aggsUsed);
        final JsonObject json = builder.build();
        return json;
    }

    @Override
    public String type(){
        return InterpreterResult.Type.UPLOT.label;
    }
}
