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
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.UPlotFormatOptions;
import com.teragrep.zep_01.interpreter.InterpreterException;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.functions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UPlotFormat implements  DatasetFormat{

    private final Dataset<Row> dataset;
    private final UPlotFormatOptions options;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);

    public UPlotFormat(Dataset<Row> dataset, UPlotFormatOptions options){
        this.dataset = dataset;
        this.options = options;
    }

    public JsonObject format() throws InterpreterException{

        List<String> seriesNames = options.seriesNames();
        int numGroups = seriesNames.size();
        // Spark data is defined by row-based arrays, but uPlot expects to get them in columns-based arrays
        // Transpose data so that we have: [[x-axis value,x-axis value,x-axis value,...],[[series1Value],[series1Value],[series1Value],...][[series2Value],[series2Value],[series2Value],...]]
        int columnCount = dataset.schema().size();

        List<String> timestampFieldNames = new ArrayList<>();
        for (StructField field:dataset.schema().fields()) {
            if(field.dataType().equals(DataTypes.TimestampType)){
                timestampFieldNames.add(field.name());
            }
        }

        Dataset<Row> modifiedDataset = dataset;
        for (String timestampFieldName: timestampFieldNames) {
            modifiedDataset = modifiedDataset.withColumn(timestampFieldName, org.apache.spark.sql.functions.unix_timestamp(functions.col(timestampFieldName)));
        }

        List<Row> datasetRows = modifiedDataset.collectAsList();

        List<List<Object>> transposed = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            List<Object> columnList = new ArrayList<Object>();
            transposed.add(columnList);
        }

        for(Row row : datasetRows){
            for (int i = 0; i < columnCount; i++){
                transposed.get(i).add(row.getAs(i));
            }
        }

        List<String> combinedXAxisValues = new ArrayList<String>();
        if(numGroups > 1){
            for (int i = 0; i < transposed.get(0).size(); i++) {
                StringBuilder combinedLabel = new StringBuilder();
                for (int j = 0; j < numGroups; j++) {
                    combinedLabel.append(transposed.get(j).get(i));
                    if(j+1 != numGroups){
                        combinedLabel.append(".");
                    }
                }
                combinedXAxisValues.add(combinedLabel.toString());
            }
        }
        else {
            combinedXAxisValues.add(dataset.schema().fieldNames()[0]);
        }
        List<String> distinctLabels = combinedXAxisValues.stream().distinct().collect(Collectors.toList());

        // X-axis is an array of indexes, mapped to labels,
        JsonArrayBuilder xAxisBuilder = Json.createArrayBuilder();
        for (String combinedXAxisValue:combinedXAxisValues) {
            xAxisBuilder.add(distinctLabels.indexOf(combinedXAxisValue));
        }
        JsonArray xAxis = xAxisBuilder.build();

        // y-axis contains the transposed datapoints
        JsonArrayBuilder yAxisBuilder = Json.createArrayBuilder();
        if(transposed.size() >= 1){
            for (int i = numGroups; i < transposed.size(); i++) {
                yAxisBuilder.add(Json.createArrayBuilder(transposed.get(i)));
            }
        }
        JsonArray yAxis = yAxisBuilder.build();
        JsonArray axesObject = Json.createArrayBuilder().add(xAxis).add(yAxis).build();

        // labels contains the names of each series of data in an array
        JsonArrayBuilder labelsBuilder = Json.createArrayBuilder(distinctLabels);
        JsonArray labels = labelsBuilder.build();

        // generate series names
        JsonArrayBuilder seriesBuilder = Json.createArrayBuilder();
        if(numGroups == 0){
            seriesBuilder.add(modifiedDataset.schema().names()[0]);
        }
        else {
            for (int i = 0+numGroups; i < modifiedDataset.schema().size(); i++) {
                seriesBuilder.add(modifiedDataset.schema().names()[i]);
            }
        }
        JsonArray series = seriesBuilder.build();

        // generate graph type
        String graphType = options.graphType();

        JsonObject optionsObject = Json.createObjectBuilder().add("series",series).add("labels",labels).add("graphType",graphType).build();
        JsonObject response = Json.createObjectBuilder().add("data",axesObject).add("options",optionsObject).build();
        return response;
    }
}
