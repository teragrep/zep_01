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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

public final class UPlotDatasetTransformationTest {

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

    /*
     * UPlotDatasetTransformation should transform the original dataset Into uPlot compliant format with given column names.
     * For example this source dataset using "_time" as xAxisColumn, "success" as groupBy column and "operation" and "filesModified" as value columns should create the following transformation:
     * +-------------------+---------+-------+-------------+
     * |              _time|operation|success|filesModified|
     * +-------------------+---------+-------+-------------+
     * |2025-01-01 12:00:00|   create|   true|            1|
     * |2025-01-01 12:00:00|   delete|   true|            2|
     * |2025-01-01 12:00:00|   update|   true|            1|
     * |2025-01-01 12:00:00|   create|   true|            1|
     * |2025-01-01 12:00:00|   delete|   true|            1|
     * |2025-01-01 12:00:00|   delete|  false|            4|
     * |2025-01-02 12:00:00|   delete|  false|            5|
     * |2025-01-02 12:00:00|   delete|  false|            2|
     * |2025-01-02 12:00:00|   delete|  false|            3|
     * |2025-01-02 12:00:00|   delete|  false|            1|
     * |2025-01-02 12:00:00|   update|   true|            1|
     * |2025-01-03 12:00:00|   update|  false|            1|
     * |2025-01-04 12:00:00|   delete|  false|            4|
     * |2025-01-05 12:00:00|   update|   true|            1|
     * |2025-01-07 12:00:00|   update|  false|            1|
     * |2025-01-08 12:00:00|   create|   true|            1|
     * |2025-01-09 12:00:00|   create|   true|            1|
     * |2025-01-10 12:00:00|   create|   true|            1|
     * |2025-01-11 12:00:00|   create|   true|            1|
     * |2025-01-12 12:00:00|   delete|   true|            1|
     * +-------------------+---------+-------+-------------+
     * -->
     * +-------------------+---------------+-------------------+--------------+------------------+
     * |              label|false.operation|false.filesModified|true.operation|true.filesModified|
     * +-------------------+---------------+-------------------+--------------+------------------+
     * |2025-01-01 12:00:00|         delete|                  4|        create|                 1|
     * |2025-01-02 12:00:00|         delete|                  5|        update|                 1|
     * |2025-01-03 12:00:00|         update|                  1|          null|              null|
     * |2025-01-04 12:00:00|         delete|                  4|          null|              null|
     * |2025-01-05 12:00:00|           null|               null|        update|                 1|
     * |2025-01-07 12:00:00|         update|                  1|          null|              null|
     * |2025-01-08 12:00:00|           null|               null|        create|                 1|
     * |2025-01-09 12:00:00|           null|               null|        create|                 1|
     * |2025-01-10 12:00:00|           null|               null|        create|                 1|
     * |2025-01-11 12:00:00|           null|               null|        create|                 1|
     * |2025-01-12 12:00:00|           null|               null|        delete|                 1|
     * |2025-01-13 12:00:00|         delete|                  1|          null|              null|
     * |2025-01-14 12:00:00|         delete|                  1|          null|              null|
     * |2025-01-15 12:00:00|         delete|                  1|          null|              null|
     * +-------------------+---------------+-------------------+--------------+------------------+
     */

    @Test
    public void testStandardTransformation(){
        final List<String> xAxisColumnNames = new ArrayList();
        final List<String> groupByColumnNames = new ArrayList();
        final List<String> valueColumnNames = new ArrayList();

        xAxisColumnNames.add("_time");
        groupByColumnNames.add("success");
        valueColumnNames.add("operation");
        valueColumnNames.add("filesModified");

        final UPlotDatasetTransformation transformation = new UPlotDatasetTransformation(sourceData,xAxisColumnNames,groupByColumnNames,valueColumnNames);
        final Dataset<Row> transformedDataset = transformation.apply();

        // Transformed dataset with xAxisColumnNames should create a new Column called "label" containing each unique value of columns listed in xAxisColumnNames
        final StructType transformedSchema = transformedDataset.schema();
        Assertions.assertEquals("label",transformedSchema.fieldNames()[0]);

        // Within the created "label" column, each row should contain every unique value contained in the "groupByColumn".
        final List<Row> label = transformedDataset.select(functions.col("label")).sort(functions.col("label")).collectAsList();
        Assertions.assertEquals(14,label.size());
        Assertions.assertEquals("2025-01-01 12:00:00", label.get(0).getString(0));
        Assertions.assertEquals("2025-01-02 12:00:00", label.get(1).getString(0));
        Assertions.assertEquals("2025-01-03 12:00:00", label.get(2).getString(0));
        Assertions.assertEquals("2025-01-04 12:00:00", label.get(3).getString(0));
        Assertions.assertEquals("2025-01-05 12:00:00", label.get(4).getString(0));
        Assertions.assertEquals("2025-01-07 12:00:00", label.get(5).getString(0));
        Assertions.assertEquals("2025-01-08 12:00:00", label.get(6).getString(0));
        Assertions.assertEquals("2025-01-09 12:00:00", label.get(7).getString(0));
        Assertions.assertEquals("2025-01-10 12:00:00", label.get(8).getString(0));
        Assertions.assertEquals("2025-01-11 12:00:00", label.get(9).getString(0));
        Assertions.assertEquals("2025-01-12 12:00:00", label.get(10).getString(0));
        Assertions.assertEquals("2025-01-13 12:00:00", label.get(11).getString(0));
        Assertions.assertEquals("2025-01-14 12:00:00", label.get(12).getString(0));
        Assertions.assertEquals("2025-01-15 12:00:00", label.get(13).getString(0));

        // Schema should contain an additional column for all unique combinations of every value within the columns named in "groupByColumns" and columns named in "valueColumnNames"
        Assertions.assertEquals("false.operation",transformedSchema.fieldNames()[1]);
        Assertions.assertEquals("false.filesModified",transformedSchema.fieldNames()[2]);
        Assertions.assertEquals("true.operation",transformedSchema.fieldNames()[3]);
        Assertions.assertEquals("true.filesModified",transformedSchema.fieldNames()[4]);

        // Check that transformed columns contains proper values, and that cells that wouldn't have a value in the base dataset are represented by nulls.
        final List<Row> failedOperations = transformedDataset.sort(functions.col("label")).select(functions.col("`false.operation`")).collectAsList();
        Assertions.assertEquals("delete", failedOperations.get(0).getString(0));
        Assertions.assertEquals("delete", failedOperations.get(1).getString(0));
        Assertions.assertEquals("update", failedOperations.get(2).getString(0));
        Assertions.assertEquals("delete", failedOperations.get(3).getString(0));
        Assertions.assertTrue(failedOperations.get(4).isNullAt(0));
        Assertions.assertEquals("update", failedOperations.get(5).getString(0));
        Assertions.assertTrue(failedOperations.get(6).isNullAt(0));
        Assertions.assertTrue(failedOperations.get(7).isNullAt(0));
        Assertions.assertTrue(failedOperations.get(8).isNullAt(0));
        Assertions.assertTrue(failedOperations.get(9).isNullAt(0));
        Assertions.assertTrue(failedOperations.get(10).isNullAt(0));
        Assertions.assertEquals("delete", failedOperations.get(11).getString(0));
        Assertions.assertEquals("delete", failedOperations.get(12).getString(0));
        Assertions.assertEquals("delete", failedOperations.get(13).getString(0));

        final List<Row> failedFileModificationCount = transformedDataset.sort(functions.col("label")).select(functions.col("`false.filesModified`")).collectAsList();
        Assertions.assertEquals(4, failedFileModificationCount.get(0).get(0));
        Assertions.assertEquals(5, failedFileModificationCount.get(1).get(0));
        Assertions.assertEquals(1, failedFileModificationCount.get(2).get(0));
        Assertions.assertEquals(4, failedFileModificationCount.get(3).get(0));
        Assertions.assertTrue(failedFileModificationCount.get(4).isNullAt(0));
        Assertions.assertEquals(1, failedFileModificationCount.get(5).get(0));
        Assertions.assertTrue(failedFileModificationCount.get(6).isNullAt(0));
        Assertions.assertTrue(failedFileModificationCount.get(7).isNullAt(0));
        Assertions.assertTrue(failedFileModificationCount.get(8).isNullAt(0));
        Assertions.assertTrue(failedFileModificationCount.get(9).isNullAt(0));
        Assertions.assertTrue(failedFileModificationCount.get(10).isNullAt(0));
        Assertions.assertEquals(1, failedFileModificationCount.get(11).get(0));
        Assertions.assertEquals(1, failedFileModificationCount.get(12).get(0));
        Assertions.assertEquals(1, failedFileModificationCount.get(13).get(0));

        final List<Row> successfulOperations = transformedDataset.sort(functions.col("label")).select(functions.col("`true.operation`")).collectAsList();
        Assertions.assertEquals("create", successfulOperations.get(0).getString(0));
        Assertions.assertEquals("update", successfulOperations.get(1).getString(0));
        Assertions.assertTrue(successfulOperations.get(2).isNullAt(0));
        Assertions.assertTrue(successfulOperations.get(3).isNullAt(0));
        Assertions.assertEquals("update", successfulOperations.get(4).getString(0));
        Assertions.assertTrue(successfulOperations.get(5).isNullAt(0));
        Assertions.assertEquals("create", successfulOperations.get(6).getString(0));
        Assertions.assertEquals("create", successfulOperations.get(7).getString(0));
        Assertions.assertEquals("create", successfulOperations.get(8).getString(0));
        Assertions.assertEquals("create", successfulOperations.get(9).getString(0));
        Assertions.assertEquals("delete", successfulOperations.get(10).getString(0));
        Assertions.assertTrue(successfulOperations.get(11).isNullAt(0));
        Assertions.assertTrue(successfulOperations.get(12).isNullAt(0));
        Assertions.assertTrue(successfulOperations.get(13).isNullAt(0));

        final List<Row> successfulFileModifications = transformedDataset.sort(functions.col("label")).select(functions.col("`true.filesModified`")).collectAsList();
        Assertions.assertEquals(1, successfulFileModifications.get(0).get(0));
        Assertions.assertEquals(1, successfulFileModifications.get(1).get(0));
        Assertions.assertTrue(successfulFileModifications.get(2).isNullAt(0));
        Assertions.assertTrue(successfulFileModifications.get(3).isNullAt(0));
        Assertions.assertEquals(1, successfulFileModifications.get(4).get(0));
        Assertions.assertTrue(successfulFileModifications.get(5).isNullAt(0));
        Assertions.assertEquals(1, successfulFileModifications.get(6).get(0));
        Assertions.assertEquals(1, successfulFileModifications.get(7).get(0));
        Assertions.assertEquals(1, successfulFileModifications.get(8).get(0));
        Assertions.assertEquals(1, successfulFileModifications.get(9).get(0));
        Assertions.assertEquals(1, successfulFileModifications.get(10).get(0));
        Assertions.assertTrue(successfulFileModifications.get(11).isNullAt(0));
        Assertions.assertTrue(successfulFileModifications.get(12).isNullAt(0));
        Assertions.assertTrue(successfulFileModifications.get(13).isNullAt(0));
    }

    /**
     * If given no arguments, transformation should return the same data as source dataset, but with an additional "label" column
     */
    @Test
    public void testNoArguments(){
        final List<String> xAxisColumnNames = new ArrayList();
        final List<String> groupByColumnNames = new ArrayList();
        final List<String> valueColumnNames = new ArrayList();

        final UPlotDatasetTransformation transformation = new UPlotDatasetTransformation(sourceData,xAxisColumnNames,groupByColumnNames,valueColumnNames);
        final Dataset<Row> transformedDataset = transformation.apply();
        Assertions.assertEquals(sourceData,transformedDataset);
    }

    @Test
    public void testContract(){
        EqualsVerifier.forClass(UPlotDatasetTransformation.class);
    }
}