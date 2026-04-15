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

import com.teragrep.pth_07.performance.metric.*;
import com.teragrep.pth_07.performance.metric.ArchiveCompressedBytesProcessedStub;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DPLPerformanceEntry {
    private final StructType schema;
    private final RowsReadFromArchive rowsReadFromArchive;
    private final BatchId batchId;
    private final Eps eps;
    private final ArchiveDatabaseRowCount archiveDatabaseRowCount;
    private final KafkaOffset kafkaOffset;
    private final BytesPerSecond bytesPerSecond;
    private final BytesProcessed bytesProcessed;
    private final LatestKafkaTimestamp latestKafkaTimestamp;
    private final ArchiveCompressedBytesProcessed archiveCompressedBytesProcessed;
    private final ArchiveObjectsProcessed archiveObjectsProcessed;
    private final ArchiveDatabaseRowMinLatency archiveDatabaseRowMinLatency;
    private final ArchiveDatabaseRowMaxLatency archiveDatabaseRowMaxLatency;
    private final ArchiveOffset archiveOffset;
    private final RecordsProcessed recordsProcessed;
    private final RecordsPerSecond recordsPerSecond;
    private final ArchiveDatabaseRowAvgLatency archiveDatabaseRowAvgLatency;
    private final Timestamp timestamp;

    public DPLPerformanceEntry(){
        this(new ArchiveCompressedBytesProcessedStub(),
                new ArchiveDatabaseRowCountStub(),
                new ArchiveDatabaseRowAvgLatencyStub(),
                new ArchiveDatabaseRowMaxLatencyStub(),
                new ArchiveDatabaseRowMinLatencyStub(),
                new ArchiveObjectsProcessedStub(),
                new ArchiveOffsetStub(),
                new BatchIdStub(),
                new BytesPerSecondStub(),
                new BytesProcessedStub(),
                new EpsStub(),
                new KafkaOffsetStub(),
                new LatestKafkaTimestampStub(),
                new RecordsPerSecondStub(),
                new RecordsProcessedStub(),
                new RowsReadFromArchiveStub(),
                new TimestampStub());
    }

    public DPLPerformanceEntry(final ArchiveCompressedBytesProcessed archiveCompressedBytesProcessed,
                               final ArchiveDatabaseRowCount archiveDatabaseRowCount,
                               final ArchiveDatabaseRowAvgLatency archiveDatabaseRowAvgLatency,
                               final ArchiveDatabaseRowMaxLatency archiveDatabaseRowMaxLatency,
                               final ArchiveDatabaseRowMinLatency archiveDatabaseRowMinLatency,
                               final ArchiveObjectsProcessed archiveObjectsProcessed,
                               final ArchiveOffset archiveOffset,
                               final BatchId batchId,
                               final BytesPerSecond bytesPerSecond,
                               final BytesProcessed bytesProcessed,
                               final Eps eps,
                               final KafkaOffset kafkaOffset,
                               final LatestKafkaTimestamp latestKafkaTimestamp,
                               final RecordsPerSecond recordsPerSecond,
                               final RecordsProcessed recordsProcessed,
                               final RowsReadFromArchive rowsReadFromArchive,
                               final Timestamp timestamp){
        this.archiveCompressedBytesProcessed = archiveCompressedBytesProcessed;
        this.archiveDatabaseRowCount = archiveDatabaseRowCount;
        this.archiveDatabaseRowAvgLatency = archiveDatabaseRowAvgLatency;
        this.archiveDatabaseRowMaxLatency = archiveDatabaseRowMaxLatency;
        this.archiveDatabaseRowMinLatency = archiveDatabaseRowMinLatency;
        this.archiveOffset = archiveOffset;
        this.archiveObjectsProcessed = archiveObjectsProcessed;
        this.batchId = batchId;
        this.bytesPerSecond = bytesPerSecond;
        this.bytesProcessed = bytesProcessed;
        this.eps = eps;
        this.kafkaOffset = kafkaOffset;
        this.latestKafkaTimestamp = latestKafkaTimestamp;
        this.recordsPerSecond = recordsPerSecond;
        this.recordsProcessed = recordsProcessed;
        this.rowsReadFromArchive = rowsReadFromArchive;
        this.timestamp = timestamp;
        this.schema = DataTypes.createStructType(new StructField[]{
                archiveCompressedBytesProcessed.structField(),
                archiveDatabaseRowCount.structField(),
                archiveDatabaseRowAvgLatency.structField(),
                archiveDatabaseRowMaxLatency.structField(),
                archiveDatabaseRowMinLatency.structField(),
                archiveOffset.structField(),
                archiveObjectsProcessed.structField(),
                batchId.structField(),
                bytesPerSecond.structField(),
                bytesProcessed.structField(),
                eps.structField(),
                kafkaOffset.structField(),
                latestKafkaTimestamp.structField(),
                recordsPerSecond.structField(),
                recordsProcessed.structField(),
                rowsReadFromArchive.structField(),
                timestamp.structField()
        });
    }

    public DPLPerformanceEntry withData(final String key, final String value){
        try{
            final DPLPerformanceEntry modifiedPerformanceEntry;
            if(key.equals(archiveCompressedBytesProcessed.name()+": "+archiveCompressedBytesProcessed.description())){
                modifiedPerformanceEntry = withArchiveCompressedBytesProcessed(Long.parseLong(value));
            }
            else if(key.equals(archiveDatabaseRowCount.name()+": "+archiveDatabaseRowCount.description())){
                modifiedPerformanceEntry = withArchiveDatabaseRowCount(Long.parseLong(value));
            }
            else if(key.equals(archiveDatabaseRowAvgLatency.name()+": "+archiveDatabaseRowAvgLatency.description())){
                modifiedPerformanceEntry = withArchiveDatabaseRowAvgLatency(Long.parseLong(value));
            }
            else if(key.equals(archiveDatabaseRowMinLatency.name()+": "+archiveDatabaseRowMinLatency.description())){
                modifiedPerformanceEntry = withArchiveDatabaseRowMinLatency(Long.parseLong(value));
            }
            else if(key.equals(archiveDatabaseRowMaxLatency.name()+": "+archiveDatabaseRowMaxLatency.description())){
                modifiedPerformanceEntry = withArchiveDatabaseRowMaxLatency(Long.parseLong(value));
            }
            else if(key.equals(archiveOffset.name()+": "+archiveOffset.description())){
                modifiedPerformanceEntry = withArchiveOffset(Long.parseLong(value));
            }
            else if(key.equals(archiveObjectsProcessed.name()+": "+archiveObjectsProcessed.description())){
                modifiedPerformanceEntry = withArchiveObjectsProcessed(Long.parseLong(value));
            }
            else if(key.equals(batchId.name()+": "+batchId.description())){
                modifiedPerformanceEntry = withBatchId(Long.parseLong(value));
            }
            else if(key.equals(bytesPerSecond.name()+": "+bytesPerSecond.description())){
                modifiedPerformanceEntry = withBytesPerSecond(Long.parseLong(value));
            }
            else if(key.equals(bytesProcessed.name()+": "+bytesProcessed.description())){
                modifiedPerformanceEntry = withBytesProcessed(Long.parseLong(value));
            }
            else if(key.equals(eps.name()+": "+eps.description())){
                modifiedPerformanceEntry = withEps(Long.parseLong(value));
            }
            else if(key.equals(kafkaOffset.name()+": "+kafkaOffset.description())){
                modifiedPerformanceEntry = withKafkaOffset(Long.parseLong(value));
            }
            else if(key.equals(latestKafkaTimestamp.name()+": "+latestKafkaTimestamp.description())){
                modifiedPerformanceEntry = withLatestKafkaTimestamp(Long.parseLong(value));
            }
            else if(key.equals(recordsPerSecond.name()+": "+recordsPerSecond.description())){
                modifiedPerformanceEntry = withRecordsPerSecond(Long.parseLong(value));
            }
            else if(key.equals(recordsProcessed.name()+": "+recordsProcessed.description())){
                modifiedPerformanceEntry = withRecordsProcessed(Long.parseLong(value));
            }
            else if(key.equals(rowsReadFromArchive.name()+": "+rowsReadFromArchive.description())){
                modifiedPerformanceEntry = withRowsReadFromArchive(Long.parseLong(value));
            }
            else if(key.equals(timestamp.name()+": "+timestamp.description())){
                modifiedPerformanceEntry = withTimestamp(Long.parseLong(value));
            }
            else {
                modifiedPerformanceEntry = this;
            }
            return modifiedPerformanceEntry;
        }
        catch (NumberFormatException numberFormatException){
            throw new UnsupportedOperationException("Failed to update performance data! Encountered invalid data type", numberFormatException);
        }
    }

    public DPLPerformanceEntry withArchiveCompressedBytesProcessed(final long value){
        return new DPLPerformanceEntry(
                new ArchiveCompressedBytesProcessedImpl(value),
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveDatabaseRowCount(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                new ArchiveDatabaseRowCountImpl(value),
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveDatabaseRowAvgLatency(final long value){
        return new DPLPerformanceEntry(archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                new ArchiveDatabaseRowAvgLatencyImpl(value),
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveDatabaseRowMaxLatency(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                new ArchiveDatabaseRowMaxLatencyImpl(value),
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveDatabaseRowMinLatency(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                new ArchiveDatabaseRowMinLatencyImpl(value),
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveObjectsProcessed(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                new ArchiveObjectsProcessedImpl(value),
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withArchiveOffset(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                new ArchiveOffsetImpl(value),
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withBatchId(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                new BatchIdImpl(value),
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withBytesPerSecond(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                new BytesPerSecondImpl(value),
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withBytesProcessed(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                new BytesProcessedImpl(value),
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withEps(final double value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                new EpsImpl(value),
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withKafkaOffset(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                new KafkaOffsetImpl(value),
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withLatestKafkaTimestamp(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                new LatestKafkaTimestampImpl(value),
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withRecordsPerSecond(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                new RecordsPerSecondImpl(value),
                recordsProcessed,
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withRecordsProcessed(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                new RecordsProcessedImpl(value),
                rowsReadFromArchive,
                timestamp);
    }
    public DPLPerformanceEntry withRowsReadFromArchive(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                new RowsReadFromArchiveImpl(value),
                timestamp);
    }
    public DPLPerformanceEntry withTimestamp(final long value){
        return new DPLPerformanceEntry(
                archiveCompressedBytesProcessed,
                archiveDatabaseRowCount,
                archiveDatabaseRowAvgLatency,
                archiveDatabaseRowMaxLatency,
                archiveDatabaseRowMinLatency,
                archiveObjectsProcessed,
                archiveOffset,
                batchId,
                bytesPerSecond,
                bytesProcessed,
                eps,
                kafkaOffset,
                latestKafkaTimestamp,
                recordsPerSecond,
                recordsProcessed,
                rowsReadFromArchive
                ,new TimestampImpl(value));
    }

    public Row asRow(){
        final List<Object> values = new ArrayList<>();
        values.add(archiveCompressedBytesProcessed.isStub() ? null : archiveCompressedBytesProcessed.value());
        values.add(archiveDatabaseRowCount.isStub() ? null : archiveDatabaseRowCount.value());
        values.add(archiveDatabaseRowAvgLatency.isStub() ? null : archiveDatabaseRowAvgLatency.value());
        values.add(archiveDatabaseRowMaxLatency.isStub() ? null : archiveDatabaseRowMaxLatency.value());
        values.add(archiveDatabaseRowMinLatency.isStub() ? null : archiveDatabaseRowMinLatency.value());
        values.add(archiveOffset.isStub() ? null : archiveOffset.value());
        values.add(archiveObjectsProcessed.isStub() ? null : archiveObjectsProcessed.value());
        values.add(batchId.isStub() ? null : batchId.value());
        values.add(bytesPerSecond.isStub() ? null : bytesPerSecond.value());
        values.add(bytesProcessed.isStub() ? null : bytesProcessed.value());
        values.add(eps.isStub() ? null : eps.value());
        values.add(kafkaOffset.isStub() ? null : kafkaOffset.value());
        values.add(latestKafkaTimestamp.isStub() ? null : latestKafkaTimestamp.value());
        values.add(recordsPerSecond.isStub() ? null : recordsPerSecond.value());
        values.add(recordsProcessed.isStub() ? null : recordsProcessed.value());
        values.add(rowsReadFromArchive.isStub() ? null : rowsReadFromArchive.value());
        values.add(timestamp.isStub() ? null : timestamp.value());
        return RowFactory.create(values.toArray());
    }
    
    public StructType schema(){
        return schema;
    }
}
