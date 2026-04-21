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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class PerformanceSchema {
    private final List<PerformanceMetric> metrics;

    public PerformanceSchema(){
        this(Arrays.asList(
            new ArchiveCompressedBytesProcessed(),
            new ArchiveDatabaseRowAvgLatency(),
            new ArchiveDatabaseRowCount(),
            new ArchiveDatabaseRowMaxLatency(),
            new ArchiveDatabaseRowMinLatency(),
            new ArchiveObjectsProcessed(),
            new ArchiveOffset(),
            new BatchId(),
            new BytesPerSecond(),
            new BytesProcessed(),
            new Eps(),
            new KafkaOffset(),
            new LatestKafkaTimestamp(),
            new RecordsPerSecond(),
            new RecordsProcessed(),
            new RowsReadFromArchive(),
            new Timestamp())
        );
    }

    public PerformanceSchema(List<PerformanceMetric> metrics){
        this.metrics = metrics;
    }
    public List<PerformanceMetric> metrics(){
        return metrics;
    }
    public StructType sparkSchema(){
        List<StructField> fields = new ArrayList<>();
        for (PerformanceMetric metric: metrics) {
            fields.add(metric.structField());
        }
        return DataTypes.createStructType(fields);
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerformanceSchema that = (PerformanceSchema) o;
        return Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics);
    }
}
