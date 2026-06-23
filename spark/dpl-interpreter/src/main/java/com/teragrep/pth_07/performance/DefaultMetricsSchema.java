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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the default metrics, including name, datatype and metadata a query performance update is expected to have.
 */
public final class DefaultMetricsSchema implements MetricsSchema {
    @Override
    public Map<String, PerformanceMetric> metricsAsMap() {
        final Map<String, PerformanceMetric> metrics = new HashMap<>();
        metrics.put("ArchiveCompressedBytesProcessed: total compressed bytes processed from archive",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveCompressedBytesProcessed: total compressed bytes processed from archive", Metadata.empty(),false));
        metrics.put("ArchiveDatabaseRowAvgLatency: average time per row in nanoseconds",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveDatabaseRowAvgLatency: average time per row in nanoseconds", Metadata.empty(),false));
        metrics.put("ArchiveDatabaseRowCount: number of processed archive database rows",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveDatabaseRowCount: number of processed archive database rows", Metadata.empty(),false));
        metrics.put("ArchiveDatabaseRowMaxLatency: maximum time per row in nanoseconds",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveDatabaseRowMaxLatency: maximum time per row in nanoseconds", Metadata.empty(),false));
        metrics.put("ArchiveDatabaseRowMinLatency: minimum time per row in nanoseconds",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveDatabaseRowMinLatency: minimum time per row in nanoseconds", Metadata.empty(),false));
        metrics.put("ArchiveObjectsProcessed: total objects processed from archive",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveObjectsProcessed: total objects processed from archive", Metadata.empty(),false));
        metrics.put("ArchiveOffset: latest archive offset processed (epoch time)",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"ArchiveOffset: latest archive offset processed (epoch time)", Metadata.empty(),false));
        metrics.put("BatchId: sequence number of the batch",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"BatchId: sequence number of the batch", Metadata.empty(),false));
        metrics.put("BytesPerSecond: processed bytes per second",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"BytesPerSecond: processed bytes per second", Metadata.empty(),false));
        metrics.put("BytesProcessed: total bytes processed",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"BytesProcessed: total bytes processed", Metadata.empty(),false));
        metrics.put("Eps: processed rows per second",new PerformanceMetric(new MetricValueStub(), DataTypes.DoubleType,"Eps: processed rows per second", Metadata.empty(),false));
        metrics.put("KafkaOffset: sum of processed kafka offsets",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"KafkaOffset: sum of processed kafka offsets", Metadata.empty(),false));
        metrics.put("LatestKafkaTimestamp: latest processed kafka records' timestamp",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"LatestKafkaTimestamp: latest processed kafka records' timestamp", Metadata.empty(),false));
        metrics.put("RecordsPerSecond: processed records per second",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"RecordsPerSecond: processed records per second", Metadata.empty(),false));
        metrics.put("RecordsProcessed: total processed records",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"RecordsProcessed: total processed records", Metadata.empty(),false));
        metrics.put("RowsReadFromArchive: Full table input rows read from arcihve",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"RowsReadFromArchive: Full table input rows read from archive", Metadata.empty(),false));
        metrics.put("Timestamp: timestamp of when performance data was received(epochtime)",new PerformanceMetric(new MetricValueStub(), DataTypes.LongType,"Timestamp: timestamp of when performance data was received(epochtime)", new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build(),false));
        return metrics;
    }
}
