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
package com.teragrep.pth_07.performance.metric;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PerformanceSchemaTest {

    @Test
    void testDefaultMetrics() {
        final PerformanceSchema performanceSchema = new PerformanceSchema();
        final int expectedSchemaSize = 17;
        Assertions.assertEquals(expectedSchemaSize,performanceSchema.metrics().size());

        // Default values should all be stubs
        int i = 0;
        while (i < expectedSchemaSize) {
            final int metricIndex = i;
            Assertions.assertThrows(UnsupportedOperationException.class, ()-> performanceSchema.metrics().get(metricIndex).metricValue().value());
            i++;
        }
        Assertions.assertEquals(i,expectedSchemaSize);

        // Metrics list should contain all supported metrics.
        Assertions.assertEquals("ArchiveCompressedBytesProcessed",performanceSchema.metrics().get(0).name());
        Assertions.assertEquals("ArchiveDatabaseRowAvgLatency",performanceSchema.metrics().get(1).name());
        Assertions.assertEquals("ArchiveDatabaseRowCount",performanceSchema.metrics().get(2).name());
        Assertions.assertEquals("ArchiveDatabaseRowMaxLatency",performanceSchema.metrics().get(3).name());
        Assertions.assertEquals("ArchiveDatabaseRowMinLatency",performanceSchema.metrics().get(4).name());
        Assertions.assertEquals("ArchiveObjectsProcessed",performanceSchema.metrics().get(5).name());
        Assertions.assertEquals("ArchiveOffset",performanceSchema.metrics().get(6).name());
        Assertions.assertEquals("BatchId",performanceSchema.metrics().get(7).name());
        Assertions.assertEquals("BytesPerSecond",performanceSchema.metrics().get(8).name());
        Assertions.assertEquals("BytesProcessed",performanceSchema.metrics().get(9).name());
        Assertions.assertEquals("Eps",performanceSchema.metrics().get(10).name());
        Assertions.assertEquals("KafkaOffset",performanceSchema.metrics().get(11).name());
        Assertions.assertEquals("LatestKafkaTimestamp",performanceSchema.metrics().get(12).name());
        Assertions.assertEquals("RecordsPerSecond",performanceSchema.metrics().get(13).name());
        Assertions.assertEquals("RecordsProcessed",performanceSchema.metrics().get(14).name());
        Assertions.assertEquals("RowsReadFromArchive",performanceSchema.metrics().get(15).name());
        Assertions.assertEquals("Timestamp",performanceSchema.metrics().get(16).name());
    }

    @Test
    void testSparkSchema() {
        final PerformanceSchema performanceSchema = new PerformanceSchema();
        final StructType schema = performanceSchema.sparkSchema();

        // Spark schema should contain all supported metrics
        Assertions.assertEquals(17,schema.size());
        Assertions.assertTrue(schema.contains(new ArchiveCompressedBytesProcessed().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveDatabaseRowAvgLatency().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveDatabaseRowCount().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveDatabaseRowMaxLatency().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveDatabaseRowMinLatency().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveObjectsProcessed().structField()));
        Assertions.assertTrue(schema.contains(new ArchiveOffset().structField()));
        Assertions.assertTrue(schema.contains(new BatchId().structField()));
        Assertions.assertTrue(schema.contains(new BytesPerSecond().structField()));
        Assertions.assertTrue(schema.contains(new BytesProcessed().structField()));
        Assertions.assertTrue(schema.contains(new Eps().structField()));
        Assertions.assertTrue(schema.contains(new KafkaOffset().structField()));
        Assertions.assertTrue(schema.contains(new LatestKafkaTimestamp().structField()));
        Assertions.assertTrue(schema.contains(new RecordsPerSecond().structField()));
        Assertions.assertTrue(schema.contains(new RecordsProcessed().structField()));
        Assertions.assertTrue(schema.contains(new RowsReadFromArchive().structField()));
        Assertions.assertTrue(schema.contains(new Timestamp().structField()));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(PerformanceSchema.class).verify();
    }
}