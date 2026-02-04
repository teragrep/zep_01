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
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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

        // Take X columns beginning from left to get access to group by series names. The remaining columns represent the data. Separate these two into different datasets.
        List<String> seriesLabels = options.seriesLabels();
        String[] seriesLabelsArray = seriesLabels.toArray(new String[0]);
        final Dataset<Row> seriesLabelDataset;
        final Dataset<Row> remainingDataset;

        if(seriesLabelsArray.length > 1){
            String firstColumn = seriesLabelsArray[0];
            String[] additionalColumns = Arrays.copyOfRange(seriesLabelsArray,1,seriesLabelsArray.length);
            seriesLabelDataset = dataset.select(firstColumn, additionalColumns);

            String firstDataColumn = dataset.schema().names()[seriesLabels.size()];
            String[] additionalDataColumns = Arrays.copyOfRange(dataset.schema().names(),seriesLabels.size()+1,dataset.schema().names().length);
            remainingDataset = dataset.select(firstDataColumn, additionalDataColumns);
        }
        else if(seriesLabelsArray.length == 1){
            String firstColumn = seriesLabelsArray[0];
            seriesLabelDataset = dataset.select(firstColumn);

            String firstDataColumn = dataset.schema().names()[seriesLabels.size()];
            String[] additionalDataColumns = Arrays.copyOfRange(dataset.schema().names(),seriesLabels.size()+1,dataset.schema().names().length);
            remainingDataset = dataset.select(firstDataColumn, additionalDataColumns);
        }
        else {
            seriesLabelDataset = dataset.select();
            remainingDataset = dataset;
        }

        // concatenate every group by row -- > options.labels
        Column[] groupByColumns = Arrays.asList(seriesLabelsArray)
                .stream().map(columnName -> org.apache.spark.sql.functions.col(columnName))
                .collect(Collectors.toList())
                .toArray(new Column[0]);
        JsonArrayBuilder labelsBuilder;
        // concat_ws will add one empty name to the dataset even if the size of provided columns is 0, so we have to fork the execution here.
        if(groupByColumns.length > 0){
            Dataset<Row> concatenatedDataset = seriesLabelDataset.withColumn("seriesName", org.apache.spark.sql.functions.concat_ws(".", groupByColumns)).select("seriesName");
            List<String> concatenatedLabels = new ArrayList<>();
            for (Row row:concatenatedDataset.collectAsList()) {
                for (int i = 0; i < row.size(); i++) {
                    concatenatedLabels.add(row.getString(i));
                }
            }
            labelsBuilder = Json.createArrayBuilder(concatenatedLabels);
        }
        else {
            labelsBuilder = Json.createArrayBuilder();
        }
        JsonArray labels = labelsBuilder.build();

        // iterate over labels, put index into new list --> data[0]
        List<Integer> seriesNameIndexes = new ArrayList<>();
        for (int i = 0; i < labels.size(); i++) {
            seriesNameIndexes.add(i);
        }
        JsonArray data0 = Json.createArrayBuilder(seriesNameIndexes).build();

        // take remaining column names --> options.series
        String[] dataNames = remainingDataset.schema().fieldNames();
        List<String> datanamesList = Arrays.asList(dataNames);
        JsonArray series = Json.createArrayBuilder(datanamesList).build();

        // transpose remaining data --> data[1]
        List<List<Object>> transposed = new ArrayList<>();
        int columnCount = remainingDataset.schema().size();
        List<Row> dataRows = remainingDataset.collectAsList();
        for (int i = 0; i < columnCount; i++) {
            List<Object> columnList = new ArrayList<Object>();
            transposed.add(columnList);
        }
        for(Row row : dataRows){
            for (int i = 0; i < columnCount; i++){
                transposed.get(i).add(row.getAs(i));
            }
        }
        JsonArray data1 = Json.createArrayBuilder(transposed).build();

        // get graph type --> options.graphType
        String graphType = options.graphType();

        // finally, create a JSON object
        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(data0)
                        .add(data1))
                .add("options",Json.createObjectBuilder()
                        .add("labels",labels)
                        .add("series",series)
                        .add("graphType",graphType));
        JsonObject json = builder.build();
        return json;
    }
}
