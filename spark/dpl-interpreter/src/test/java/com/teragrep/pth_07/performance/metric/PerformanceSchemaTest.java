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

import com.teragrep.pth_07.performance.metric.value.StubMetricValue;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

        PerformanceMetric<Long> archiveCompressedBytesProcessed =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveCompressedBytesProcessed","total compressed bytes processed from archive", DataTypes.LongType, Metadata.empty(),false);
        PerformanceMetric<Long> archiveDatabaseRowAvgLatency =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveDatabaseRowAvgLatency","average time per row in nanoseconds",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> archiveDatabaseRowCount =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveDatabaseRowCount","number of processed archive database rows",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> archiveDatabaseRowMaxLatency =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveDatabaseRowMaxLatency","maximum time per row in nanoseconds",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> archiveDatabaseRowMinLatency =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveDatabaseRowMinLatency","minimum time per row in nanoseconds",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> archiveObjectsProcessed =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveObjectsProcessed","total objects processed from archive",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> archiveOffset =  new PerformanceMetric<Long>(new StubMetricValue<>(),"ArchiveOffset","latest archive offset processed (epoch time)",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> batchId =  new PerformanceMetric<Long>(new StubMetricValue<>(),"BatchId","sequence number of the batch",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> bytesPerSecond =  new PerformanceMetric<Long>(new StubMetricValue<>(),"BytesPerSecond","processed bytes per second",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> bytesProcessed =  new PerformanceMetric<Long>(new StubMetricValue<>(),"BytesProcessed","total bytes processed",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Double> eps =  new PerformanceMetric<Double>(new StubMetricValue<>(),"Eps","processed rows per second",DataTypes.DoubleType,Metadata.empty(),false);
        PerformanceMetric<Long> kafkaOffset =  new PerformanceMetric<Long>(new StubMetricValue<>(),"KafkaOffset","sum of processed kafka offsets",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> katestKafkaTimestamp =  new PerformanceMetric<Long>(new StubMetricValue<>(),"LatestKafkaTimestamp","latest processed kafka records' timestamp",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> recordsPerSecond =  new PerformanceMetric<Long>(new StubMetricValue<>(),"RecordsPerSecond","processed records per second",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> recordsProcessed =  new PerformanceMetric<Long>(new StubMetricValue<>(),"RecordsProcessed","total processed records",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> rowsReadFromArchive =  new PerformanceMetric<Long>(new StubMetricValue<>(),"RowsReadFromArchive","Full table input rows read from archive",DataTypes.LongType,Metadata.empty(),false);
        PerformanceMetric<Long> timestamp =  new PerformanceMetric<Long>(new StubMetricValue<>(),"Timestamp","timestamp of when performance data was received(epochtime)",DataTypes.LongType, new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build(),false);

        // Spark schema should contain all supported metrics
        Assertions.assertEquals(17,schema.size());
        Assertions.assertTrue(schema.contains(archiveCompressedBytesProcessed.toStructField()));
        Assertions.assertTrue(schema.contains(archiveDatabaseRowAvgLatency.toStructField()));
        Assertions.assertTrue(schema.contains(archiveDatabaseRowCount.toStructField()));
        Assertions.assertTrue(schema.contains(archiveDatabaseRowMaxLatency.toStructField()));
        Assertions.assertTrue(schema.contains(archiveDatabaseRowMinLatency.toStructField()));
        Assertions.assertTrue(schema.contains(archiveObjectsProcessed.toStructField()));
        Assertions.assertTrue(schema.contains(archiveOffset.toStructField()));
        Assertions.assertTrue(schema.contains(batchId.toStructField()));
        Assertions.assertTrue(schema.contains(bytesPerSecond.toStructField()));
        Assertions.assertTrue(schema.contains(bytesProcessed.toStructField()));
        Assertions.assertTrue(schema.contains(eps.toStructField()));
        Assertions.assertTrue(schema.contains(kafkaOffset.toStructField()));
        Assertions.assertTrue(schema.contains(katestKafkaTimestamp.toStructField()));
        Assertions.assertTrue(schema.contains(recordsPerSecond.toStructField()));
        Assertions.assertTrue(schema.contains(recordsProcessed.toStructField()));
        Assertions.assertTrue(schema.contains(rowsReadFromArchive.toStructField()));
        Assertions.assertTrue(schema.contains(timestamp.toStructField()));
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(PerformanceSchema.class).verify();
    }
}