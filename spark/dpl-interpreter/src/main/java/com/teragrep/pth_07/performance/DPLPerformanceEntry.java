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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DPLPerformanceEntry {
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
        this(new ArchiveCompressedBytesProcessed(),
                new ArchiveDatabaseRowCount(),
                new ArchiveDatabaseRowAvgLatency(),
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
                new Timestamp());
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
                new ArchiveCompressedBytesProcessed(value),
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
                new ArchiveDatabaseRowCount(value),
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
                new ArchiveDatabaseRowAvgLatency(value),
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
                new ArchiveDatabaseRowMaxLatency(value),
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
                new ArchiveDatabaseRowMinLatency(value),
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
                new ArchiveObjectsProcessed(value),
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
                new ArchiveOffset(value),
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
                new BatchId(value),
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
                new BytesPerSecond(value),
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
                new BytesProcessed(value),
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
                new Eps(value),
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
                new KafkaOffset(value),
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
                new LatestKafkaTimestamp(value),
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
                new RecordsPerSecond(value),
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
                new RecordsProcessed(value),
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
                new RowsReadFromArchive(value),
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
                ,new Timestamp(value));
    }

    public Row asRow(){
        return asRow(schema());
    }
    public Row asRow(StructType schema){
        final List<Object> values = new ArrayList<>();
        for (StructField field : schema.fields()) {
            if(field.name().equals(archiveCompressedBytesProcessed.name()) && field.dataType().equals(archiveCompressedBytesProcessed.type())){
                values.add(archiveCompressedBytesProcessed.value().isStub() ? null : archiveCompressedBytesProcessed.value().value());
            }
            else if(field.name().equals(archiveDatabaseRowCount.name()) && field.dataType().equals(archiveDatabaseRowCount.type())){
                values.add(archiveDatabaseRowCount.value().isStub() ? null : archiveDatabaseRowCount.value().value());
            }
            else if(field.name().equals(archiveDatabaseRowAvgLatency.name()) && field.dataType().equals(archiveDatabaseRowAvgLatency.type())){
                values.add(archiveDatabaseRowAvgLatency.value().isStub() ? null : archiveDatabaseRowAvgLatency.value().value());
            }
            else if(field.name().equals(archiveDatabaseRowMinLatency.name()) && field.dataType().equals(archiveDatabaseRowMinLatency.type())){
                values.add(archiveDatabaseRowMinLatency.value().isStub() ? null : archiveDatabaseRowMinLatency.value().value());
            }
            else if(field.name().equals(archiveDatabaseRowMaxLatency.name()) && field.dataType().equals(archiveDatabaseRowMaxLatency.type())){
                values.add(archiveDatabaseRowMaxLatency.value().isStub() ? null : archiveDatabaseRowMaxLatency.value().value());
            }
            else if(field.name().equals(archiveOffset.name()) && field.dataType().equals(archiveOffset.type())){
                values.add(archiveOffset.value().isStub() ? null : archiveOffset.value().value());
            }
            else if(field.name().equals(archiveObjectsProcessed.name()) && field.dataType().equals(archiveObjectsProcessed.type())){
                values.add(archiveObjectsProcessed.value().isStub() ? null : archiveObjectsProcessed.value().value());
            }
            else if(field.name().equals(batchId.name()) && field.dataType().equals(batchId.type())){
                values.add(batchId.value().isStub() ? null : batchId.value().value());
            }
            else if(field.name().equals(bytesPerSecond.name()) && field.dataType().equals(bytesPerSecond.type())){
                values.add(bytesPerSecond.value().isStub() ? null : bytesPerSecond.value().value());
            }
            else if(field.name().equals(bytesProcessed.name()) && field.dataType().equals(bytesProcessed.type())){
                values.add(bytesProcessed.value().isStub() ? null : bytesProcessed.value().value());
            }
            else if(field.name().equals(eps.name()) && field.dataType().equals(eps.type())){
                values.add(eps.value().isStub() ? null : eps.value().value());
            }
            else if(field.name().equals(kafkaOffset.name()) && field.dataType().equals(kafkaOffset.type())){
                values.add(kafkaOffset.value().isStub() ? null : kafkaOffset.value().value());
            }
            else if(field.name().equals(latestKafkaTimestamp.name()) && field.dataType().equals(latestKafkaTimestamp.type())){
                values.add(latestKafkaTimestamp.value().isStub() ? null : latestKafkaTimestamp.value().value());
            }
            else if(field.name().equals(recordsPerSecond.name()) && field.dataType().equals(recordsPerSecond.type())){
                values.add(recordsPerSecond.value().isStub() ? null : recordsPerSecond.value().value());
            }
            else if(field.name().equals(recordsProcessed.name()) && field.dataType().equals(recordsProcessed.type())){
                values.add(recordsProcessed.value().isStub() ? null : recordsProcessed.value().value());
            }
            else if(field.name().equals(rowsReadFromArchive.name()) && field.dataType().equals(rowsReadFromArchive.type())){
                values.add(rowsReadFromArchive.value().isStub() ? null : rowsReadFromArchive.value().value());
            }
            else if(field.name().equals(timestamp.name()) && field.dataType().equals(timestamp.type())){
                values.add(timestamp.value().isStub() ? null : timestamp.value().value());
            }
            else {
                continue;
            }
        }
        return new GenericRowWithSchema(values.toArray(),schema);
    }

    public StructType schema(){
        List<StructField> fields = new ArrayList<>();
        fields.add(archiveCompressedBytesProcessed.structField());
        fields.add(archiveDatabaseRowCount.structField());
        fields.add(archiveDatabaseRowAvgLatency.structField());
        fields.add(archiveDatabaseRowMaxLatency.structField());
        fields.add(archiveDatabaseRowMinLatency.structField());
        fields.add(archiveOffset.structField());
        fields.add(archiveObjectsProcessed.structField());
        fields.add(batchId.structField());
        fields.add(bytesPerSecond.structField());
        fields.add(bytesProcessed.structField());
        fields.add(eps.structField());
        fields.add(kafkaOffset.structField());
        fields.add(latestKafkaTimestamp.structField());
        fields.add(recordsPerSecond.structField());
        fields.add(recordsProcessed.structField());
        fields.add(rowsReadFromArchive.structField());
        fields.add(timestamp.structField());
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DPLPerformanceEntry entry = (DPLPerformanceEntry) o;
        return Objects.equals(rowsReadFromArchive, entry.rowsReadFromArchive) && Objects.equals(batchId, entry.batchId) && Objects.equals(eps, entry.eps) && Objects.equals(archiveDatabaseRowCount, entry.archiveDatabaseRowCount) && Objects.equals(kafkaOffset, entry.kafkaOffset) && Objects.equals(bytesPerSecond, entry.bytesPerSecond) && Objects.equals(bytesProcessed, entry.bytesProcessed) && Objects.equals(latestKafkaTimestamp, entry.latestKafkaTimestamp) && Objects.equals(archiveCompressedBytesProcessed, entry.archiveCompressedBytesProcessed) && Objects.equals(archiveObjectsProcessed, entry.archiveObjectsProcessed) && Objects.equals(archiveDatabaseRowMinLatency, entry.archiveDatabaseRowMinLatency) && Objects.equals(archiveDatabaseRowMaxLatency, entry.archiveDatabaseRowMaxLatency) && Objects.equals(archiveOffset, entry.archiveOffset) && Objects.equals(recordsProcessed, entry.recordsProcessed) && Objects.equals(recordsPerSecond, entry.recordsPerSecond) && Objects.equals(archiveDatabaseRowAvgLatency, entry.archiveDatabaseRowAvgLatency) && Objects.equals(timestamp, entry.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowsReadFromArchive, batchId, eps, archiveDatabaseRowCount, kafkaOffset, bytesPerSecond, bytesProcessed, latestKafkaTimestamp, archiveCompressedBytesProcessed, archiveObjectsProcessed, archiveDatabaseRowMinLatency, archiveDatabaseRowMaxLatency, archiveOffset, recordsProcessed, recordsPerSecond, archiveDatabaseRowAvgLatency, timestamp);
    }
}
