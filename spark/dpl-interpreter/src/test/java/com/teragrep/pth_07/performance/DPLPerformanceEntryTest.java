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
package com.teragrep.pth_07.performance;

import com.teragrep.pth_07.performance.metric.PerformanceMetric;
import com.teragrep.pth_07.performance.metric.value.MetricValueStub;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DPLPerformanceEntryTest {

    @Test
    void testWithData() {
        final String inputKey = "BytesPerSecond: processed bytes per second";
        final long inputValue = 512l;
        final DPLPerformanceEntry entry = new DPLPerformanceEntry(new DefaultMetricsSchema());
        final DPLPerformanceEntry modifiedEntry = Assertions.assertDoesNotThrow(()->entry.withData(inputKey,inputValue));
        final Row row = modifiedEntry.asRow();
        final int bytesPerSecondIndex = row.fieldIndex(inputKey);
        final int bytesProcessedindex = row.fieldIndex("BytesProcessed: total bytes processed");

        // BytesPerSecond should have a value, BytesProcessed should contain a null.
        Assertions.assertEquals(inputValue,row.getLong(bytesPerSecondIndex));
        Assertions.assertEquals(null,row.get(bytesProcessedindex));
    }

    @Test
    void testWithDataIgnoresUnknownKeys() {
        final String inputKey = "unknownKey: some data we want to ignore";
        final long inputValue = 52l;
        final DPLPerformanceEntry entry = new DPLPerformanceEntry(new DefaultMetricsSchema());
        final DPLPerformanceEntry modifiedEntry = Assertions.assertDoesNotThrow(()->entry.withData(inputKey,inputValue));
        final Row row = modifiedEntry.asRow();
        final int expectedRowCount = 17;

        // Created row should be fully empty, containing only null values for each of the entries
        Assertions.assertEquals(expectedRowCount,row.size());
        int i = 0;
        while (i < row.size()) {
            Assertions.assertTrue(row.isNullAt(i));
            i++;
        }
        Assertions.assertEquals(i,expectedRowCount);
    }

    @Test
    void testWithDataUsingCustomSchema() {
        final String bytesPerSecondInputKey = "BytesPerSecond: processed bytes per second";
        final long bytesPerSecondInputValue = 512l;

        final String timestampInputKey = "Timestamp: timestamp of when performance data was received(epochtime)";
        final long timestampValue = 1780000000;

        final String epsInputKey = "Eps: processed rows per second";
        final double epsValue = 2000.50;

        // Some data that does not appear in the schema
        final String recordsProcessedInputKey = "RecordsProcessed: total processed records";
        final long recordsProcessedInputValue = 500000l;

        final Map<String, PerformanceMetric> metrics = new HashMap<>();
        metrics.put(bytesPerSecondInputKey,new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,bytesPerSecondInputKey, Metadata.empty(),false));
        metrics.put(timestampInputKey,new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,timestampInputKey, Metadata.empty(),false));
        metrics.put(epsInputKey,new PerformanceMetric(new MetricValueStub(), DataTypes.DoubleType, epsInputKey, Metadata.empty(),false));
        final DPLPerformanceEntry entry = new DPLPerformanceEntry(metrics);
        DPLPerformanceEntry modifiedEntry = Assertions.assertDoesNotThrow(()->entry.withData(recordsProcessedInputKey,recordsProcessedInputValue));
        final DPLPerformanceEntry modifiedEntry2 = Assertions.assertDoesNotThrow(()-> modifiedEntry.withData(bytesPerSecondInputKey,bytesPerSecondInputValue));
        final DPLPerformanceEntry modifiedEntry3 = Assertions.assertDoesNotThrow(()-> modifiedEntry2.withData(epsInputKey,epsValue));
        final DPLPerformanceEntry modifiedEntry4 = Assertions.assertDoesNotThrow(()-> modifiedEntry3.withData(timestampInputKey,timestampValue));

        final Row row = modifiedEntry4.asRow();
        final int bytesPerSecondIndex = row.fieldIndex(bytesPerSecondInputKey);
        final int timestampIndex = row.fieldIndex(timestampInputKey);
        final int epsIndex = row.fieldIndex(epsInputKey);

        // As customSchema does not contain RecordsProcessed, it should not be included in the dataset.
        Assertions.assertThrows(IllegalArgumentException.class,()-> row.fieldIndex("RecordsProcessed"));

        // The ordering of the resulting dataset should match with the order of the given schema
        Assertions.assertEquals(0,bytesPerSecondIndex);
        Assertions.assertEquals(1,timestampIndex);
        Assertions.assertEquals(2,epsIndex);

        // values should also be present
        Assertions.assertEquals(bytesPerSecondInputValue,row.getLong(bytesPerSecondIndex));
        Assertions.assertEquals(timestampValue,row.getLong(timestampIndex));
        Assertions.assertEquals(epsValue,row.getDouble(epsIndex));
    }

    @Test
    public void datasetSchemaTest(){
        // Create some dummy data
        final String bytesPerSecondInputKey = "BytesPerSecond: processed bytes per second";
        final long bytesPerSecondInputValue = 512l;

        final String timestampInputKey = "Timestamp: timestamp of when performance data was received(epochtime)";
        final long timestampValue = 1780000000;

        final String epsInputKey = "Eps: processed rows per second";
        final double epsValue = 2000.50;

        final String recordsProcessedInputKey = "RecordsProcessed: total processed records";
        final long recordsProcessedInputValue = 500000l;

        // Create entries with default schemas and apply some values.
        DPLPerformanceEntry entry1 = new DPLPerformanceEntry(new DefaultMetricsSchema());
        entry1 = entry1.withData(bytesPerSecondInputKey,bytesPerSecondInputValue);
        DPLPerformanceEntry entry2 = new DPLPerformanceEntry(new DefaultMetricsSchema());
        entry2 = entry2.withData(timestampInputKey,timestampValue);
        DPLPerformanceEntry entry3 = new DPLPerformanceEntry(new DefaultMetricsSchema());
        entry3 = entry3.withData(epsInputKey,epsValue);
        entry3 = entry3.withData(recordsProcessedInputKey,recordsProcessedInputValue);
        entry3 = entry3.withData(bytesPerSecondInputKey,bytesPerSecondInputValue);

        final SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        List<Row> rows = new ArrayList<Row>();
        rows.add(entry1.asRow(entry1.performanceSchema()));
        rows.add(entry2.asRow(entry2.performanceSchema()));
        rows.add(entry3.asRow(entry3.performanceSchema()));

        // All rows should have indentical schemas, regardless of their assigned values
        Assertions.assertEquals(entry1.performanceSchema(),entry2.performanceSchema());
        Assertions.assertEquals(entry2.performanceSchema(),entry3.performanceSchema());

        // Dataset should be created successfully from the list of rows. If schemas don't match between rows, an Exception is thrown.
        StructType performanceSchema = entry1.performanceSchema();
        Dataset<Row> dataset = Assertions.assertDoesNotThrow(()->sparkSession.createDataFrame(rows, performanceSchema));
        Assertions.assertEquals(performanceSchema,dataset.schema());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(DPLPerformanceEntry.class).verify();
    }
}