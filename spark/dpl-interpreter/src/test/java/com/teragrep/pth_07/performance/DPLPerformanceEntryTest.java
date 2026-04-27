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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DPLPerformanceEntryTest {

    @Test
    void testWithData() {
        final String inputKey = "BytesPerSecond: processed bytes per second";
        final String inputValue = "512";
        final long expectedValue  = Long.parseLong(inputValue);
        final DPLPerformanceEntry entry = new DPLPerformanceEntry();
        final DPLPerformanceEntry modifiedEntry = Assertions.assertDoesNotThrow(()->entry.withData(inputKey,inputValue));
        final Row row = modifiedEntry.asRow();
        final int bytesPerSecondIndex = row.fieldIndex("BytesPerSecond");
        final int bytesProcessedindex = row.fieldIndex("BytesProcessed");

        // BytesPerSecond should have a value, BytesProcessed should contain a null.
        Assertions.assertEquals(expectedValue,row.getLong(bytesPerSecondIndex));
        Assertions.assertEquals(null,row.get(bytesProcessedindex));
    }

    @Test
    void testWithDataIgnoresUnknownKeys() {
        final String inputKey = "unknownKey: some data we want to ignore";
        final String inputValue = "string data";
        final DPLPerformanceEntry entry = new DPLPerformanceEntry();
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
        final String bytesPerSecondInputValue = "512";
        final long expectedBytesPersecond  = Long.parseLong(bytesPerSecondInputValue);

        final String timestampInputKey = "Timestamp: timestamp of when performance data was received(epochtime)";
        final long timestampValue = 1780000000;
        final String epsInputKey = "Eps: processed rows per second";
        final double epsValue = 2000.50;

        final String recordsProcessedInputKey = "RecordsProcessed: total processed records";
        final String recordsProcessedInputValue = "500000";

        final DPLPerformanceEntry entry = new DPLPerformanceEntry();
        DPLPerformanceEntry modifiedEntry = Assertions.assertDoesNotThrow(()->entry.withData(recordsProcessedInputKey,recordsProcessedInputValue));
        final DPLPerformanceEntry finalModifiedEntry = modifiedEntry;
        modifiedEntry = Assertions.assertDoesNotThrow(()-> finalModifiedEntry.withData(bytesPerSecondInputKey,bytesPerSecondInputValue));
        final DPLPerformanceEntry finalModifiedEntry1 = modifiedEntry;
        modifiedEntry = Assertions.assertDoesNotThrow(()-> finalModifiedEntry1.withData(epsInputKey,epsValue));
        final DPLPerformanceEntry finalModifiedEntry2 = modifiedEntry;
        modifiedEntry = Assertions.assertDoesNotThrow(()-> finalModifiedEntry2.withData(timestampInputKey,timestampValue));

        // Create a schema that contains only two of the three metrics, in different order compared to default.
        final StructType customSchema = new StructType(new StructField[]{
                new StructField("Timestamp",DataTypes.LongType,true, new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build()),
                new StructField("BytesPerSecond",DataTypes.LongType,true, new MetadataBuilder().build()),
                new StructField("Eps",DataTypes.DoubleType,true, new MetadataBuilder().build()),
        });

        final Row row = modifiedEntry.asRow(customSchema);
        final int bytesPerSecondIndex = row.fieldIndex("BytesPerSecond");
        final int timestampIndex = row.fieldIndex("Timestamp");
        final int epsIndex = row.fieldIndex("Eps");

        // As customSchema does not contain RecordsProcessed, it should not be included in the dataset.
        Assertions.assertThrows(IllegalArgumentException.class,()-> row.fieldIndex("RecordsProcessed"));

        // The ordering of the resulting dataset should match with the order of the given schema
        Assertions.assertEquals(0,timestampIndex);
        Assertions.assertEquals(1,bytesPerSecondIndex);
        Assertions.assertEquals(2,epsIndex);

        // values should also be present
        Assertions.assertEquals(expectedBytesPersecond,row.getLong(bytesPerSecondIndex));
        Assertions.assertEquals(timestampValue,row.getLong(timestampIndex));
        Assertions.assertEquals(epsValue,row.getDouble(epsIndex));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(DPLPerformanceEntry.class).verify();
    }
}