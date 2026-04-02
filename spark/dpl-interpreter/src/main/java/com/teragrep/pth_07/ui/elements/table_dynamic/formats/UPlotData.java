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
import jakarta.json.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public final class UPlotData {

    private final List<Row> collectedData;
    private final boolean aggsUsed;
    private JsonArray cachedJson;

    public UPlotData(final List<Row> collectedData, final boolean aggsUsed){
        this.collectedData = collectedData;
        this.aggsUsed = aggsUsed;
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
        if(!rows.isEmpty()){
            final StructType schema = rows.get(0).schema();
            for (int i = 0; i < schema.fields().length; i++) {
                final JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                final StructField field = schema.fields()[i];
                if(!field.metadata().contains("dpl_internal_isGroupByColumn")){
                    for (final Row row:rows) {
                        final DataType type = field.dataType();
                        final Object value = row.get(i);
                        // Spark datasets may contain null values, and uPlot expects JSON nulls to represent empty data
                        if(value == null){
                            arrayBuilder.add(JsonValue.NULL);
                        }
                        else {
                            if(type.equals(DataTypes.StringType)){
                                // Some data from DPL may come as numerical data, but stringified. If string data is encountered, try to parse into Double first.
                                try{
                                    final Double doubleValue = Double.parseDouble((String)value);
                                    arrayBuilder.add(doubleValue);
                                }
                                // Showing non-numerical data throws an error when attempting to display it on uPlot's Y axis.
                                // If parsing of an encountered string fails, we replace it with JSON null values to allow for the rest of the data to be shown.
                                catch (final NumberFormatException exception){
                                    arrayBuilder.add(JsonValue.NULL);
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
                                // uPlot throws an error if attempting to display any data that is not in numerical format.
                                // To allow drawing datasets where some columns are numerical and others are not, we show a JSON null values for non-numerical columns.
                                arrayBuilder.add(JsonValue.NULL);
                            }
                        }
                    }
                    final JsonArray array = arrayBuilder.build();
                    yAxis.add(array);
                }
            }
        }
    return yAxis;
    }

    public JsonValue asJson() throws InterpreterException {
        if(cachedJson == null){
            final JsonArray xAxis = xAxis(collectedData, aggsUsed);
            final List<JsonArray> yAxisArray = yAxis(collectedData);

            final JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
            dataBuilder.add(xAxis);
            for (final JsonArray array:yAxisArray) {
                dataBuilder.add(array);
            }
            cachedJson = dataBuilder.build();
        }
        return cachedJson;
    }
}
