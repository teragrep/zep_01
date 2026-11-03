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

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

class DataTablesDatasetTransformationTest {

    private final String sourceDataFile = "src/test/resources/formatTestSourceData.csv";
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    private final StructType schema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                    new StructField("filesModified", DataTypes.IntegerType, false, new MetadataBuilder().build())
            }
    );
    private final Dataset<Row> sourceData = sparkSession.read().option("header",true).schema(schema).csv(sourceDataFile);

    @Test
    void testSingleOrdering() {
        // order by first column (_time), descending
        List<Order> order = new ArrayList<>();
        order.add(new Order(0,"desc",sourceData.schema().size()-1));
        DataTablesDatasetTransformation transformation = new DataTablesDatasetTransformation(sourceData,order);
        Dataset<Row> modifiedDataset = transformation.order();

        // each row in modified time column should be smaller than or equal to the last one
        List<Timestamp> values = modifiedDataset.select("_time")
                .as(Encoders.TIMESTAMP())
                .collectAsList();
        Timestamp previous = new Timestamp(Long.MAX_VALUE);
        for(Timestamp date : values){
            Assertions.assertTrue(date.before(previous) || date.equals(previous));
            previous = date;
        }
    }

    @Test
    void testMultiOrdering() {
        // order by first column (_time), descending and fourth column (filesModified), ascending
        List<Order> order = new ArrayList<>();
        order.add(new Order(0,"desc", sourceData.schema().size()-1));
        order.add(new Order(3,"asc", sourceData.schema().size()-1));
        DataTablesDatasetTransformation transformation = new DataTablesDatasetTransformation(sourceData,order);
        Dataset<Row> transformedDataset = transformation.order();

        List<Timestamp> timeValues = transformedDataset.select("_time")
                .as(Encoders.TIMESTAMP())
                .collectAsList();
        List<Integer> filesModifiedValues = transformedDataset.select("filesModified")
                .as(Encoders.INT())
                .collectAsList();
        Timestamp previousTime = new Timestamp(Long.MAX_VALUE);
        int previousFilesModified = Integer.MIN_VALUE;
        for(int i = 0; i < timeValues.size(); i++){
            // _time is primarily sorted and descending, so each row should contain a smaller or equal value than previous row
            Assertions.assertTrue(timeValues.get(i).before(previousTime) || timeValues.get(i).equals(previousTime));
            if(timeValues.get(i).before(previousTime)){
                previousFilesModified = Integer.MIN_VALUE;
            }
            previousTime = timeValues.get(i);
            // filesModified is secondarily sorted and ascending, so each row should contain a larger or equal value than previous row, as long as the primarily sorted column's value hasn't changed from the previous row.
            Assertions.assertTrue(filesModifiedValues.get(i) >= previousFilesModified);
            previousFilesModified = filesModifiedValues.get(i);
        }
    }


    @Test
    void testInvalidColumnIndex() {
        // attempt to order by an index that is out of bounds. should default to the closest valid column by index
        List<Order> order = new ArrayList<>();
        order.add(new Order(-20,"desc", sourceData.schema().size()-1));
        DataTablesDatasetTransformation transformation = new DataTablesDatasetTransformation(sourceData,order);
        Dataset<Row> transformedDataset = transformation.order();

        List<Timestamp> timeValues = transformedDataset.select("_time")
                .as(Encoders.TIMESTAMP())
                .collectAsList();
        Timestamp previousTime = new Timestamp(Long.MAX_VALUE);
        for(int i = 0; i < timeValues.size(); i++){
            // _time is primarily sorted and descending, so each row should contain a smaller or equal value than previous row
            Assertions.assertTrue(timeValues.get(i).before(previousTime) || timeValues.get(i).equals(previousTime));
            previousTime = timeValues.get(i);
        }
    }
}