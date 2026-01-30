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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UPlotFormat implements  DatasetFormat{

    private final Dataset<Row> dataset;
    private final UPlotFormatOptions options;
    private static final Logger LOGGER = LoggerFactory.getLogger(UPlotFormat.class);

    public UPlotFormat(Dataset<Row> dataset, UPlotFormatOptions options){
        this.dataset = dataset;
        this.options = options;
    }

    public JsonObject format() throws InterpreterException{
        // Spark data is defined by row-based arrays, but uPlot expects to get them in columns-based arrays
        // Transpose data so that we have: [[x-axis value,x-axis value,x-axis value,...],[[series1Value],[series1Value],[series1Value],...][[series2Value],[series2Value],[series2Value],...]]
        int columnCount = dataset.schema().size();

        List<Row> datasetRows = dataset.collectAsList();

        List<List<String>> transposed = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            List<String> columnList = new ArrayList<String>();
            transposed.add(columnList);
        }

        for(Row row : datasetRows){
            for (int i = 0; i < columnCount; i++){
                transposed.get(i).add(row.get(i).toString());
            }
        }
        
        JsonArray xAxis = Json.createArrayBuilder(transposed.get(0)).build();
        JsonArrayBuilder yAxisBuilder = Json.createArrayBuilder();
        if(transposed.size() > 1){
            for (int i = 1; i < transposed.size(); i++) {
                yAxisBuilder.add(Json.createArrayBuilder(transposed.get(i)));
            }
        }
        JsonArray yAxis = yAxisBuilder.build();
        JsonObject axesObject = Json.createObjectBuilder().add("xAxis",xAxis).add("yAxis",yAxis).build();

        // TODO: generate x-axis ticks
        JsonArray labels = Json.createArrayBuilder().build();

        // generate series names
        JsonArrayBuilder seriesBuilder = Json.createArrayBuilder();
        for (String fieldName : dataset.schema().fieldNames()) {
            seriesBuilder.add(fieldName);
        }
        JsonArray series = seriesBuilder.build();

        // generate graph type
        String graphType = options.graphType();

        JsonObject optionsObject = Json.createObjectBuilder().add("labels",labels).add("series",series).add("graphType",graphType).build();
        JsonObject response = Json.createObjectBuilder().add("data",axesObject).add("options",optionsObject).build();
        return response;
    }
}
