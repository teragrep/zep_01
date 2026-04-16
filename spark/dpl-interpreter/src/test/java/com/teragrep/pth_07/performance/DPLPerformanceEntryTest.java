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
        final DPLPerformanceEntry modifiedEntry = entry.withData(inputKey,inputValue);
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
        final DPLPerformanceEntry modifiedEntry = entry.withData(inputKey,inputValue);
        final Row row = modifiedEntry.asRow();

        // Created row should be fully empty, containing only null values for each of the entries
        Assertions.assertEquals(17,row.size());
        for (int i = 0; i < row.size(); i++) {
            Assertions.assertTrue(row.isNullAt(i));
        }
    }

    @Test
    void testWithDataUsingCustomSchema() {
        final String bytesPerSecondInputKey = "BytesPerSecond: processed bytes per second";
        final String bytesPerSecondInputValue = "512";
        final long expectedBytesPersecond  = Long.parseLong(bytesPerSecondInputValue);

        final long timestampValue = 1780000000;
        final double epsValue = 2000.50;

        final String recordsProcessedInputKey = "RecordsProcessed: total processed records";
        final String recordsProcessedInputValue = "500000";

        final DPLPerformanceEntry entry = new DPLPerformanceEntry();
        DPLPerformanceEntry modifiedEntry = entry.withData(recordsProcessedInputKey,recordsProcessedInputValue);
        modifiedEntry = modifiedEntry.withData(bytesPerSecondInputKey,bytesPerSecondInputValue);
        modifiedEntry = modifiedEntry.withEps(epsValue);
        modifiedEntry = modifiedEntry.withTimestamp(timestampValue);

        // Create a schema that contains only two of the three metrics, in different order compared to default.
        StructType customSchema = new StructType(new StructField[]{
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