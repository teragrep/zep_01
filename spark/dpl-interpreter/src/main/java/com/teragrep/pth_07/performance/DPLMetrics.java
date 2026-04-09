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

import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Enum of all supported DPL performance metrics.
 * Each entry defines a name, a Spark data type and Spark Metadata that might be needed.
 * Can generate a schema containing each entry.
 */
public enum DPLMetrics {
    rowsReadFromArchive("rowsReadFromArchive", DataTypes.IntegerType, new MetadataBuilder().build()),
    batchId("batchId",DataTypes.IntegerType, new MetadataBuilder().build()),
    EPS("EPS",DataTypes.DoubleType, new MetadataBuilder().build()),
    archiveDatabaseRowCount("archiveDatabaseRowCount",DataTypes.IntegerType, new MetadataBuilder().build()),
    kafkaOffset("kafkaOffset",DataTypes.IntegerType, new MetadataBuilder().build()),
    bytesPerSecond("bytesPerSecond",DataTypes.IntegerType, new MetadataBuilder().build()),
    bytesProcessed("bytesProcessed",DataTypes.IntegerType, new MetadataBuilder().build()),
    latestKafkaTimestamp("latestKafkaTimestamp",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveCompressedBytesProcessed("archiveCompressedBytesProcessed",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveObjectsProcessed("archiveObjectsProcessed",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveDatabaseRowMinLatency("archiveDatabaseRowMinLatency",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveDatabaseRowMaxLatency("archiveDatabaseRowMaxLatency",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveOffset("archiveOffset",DataTypes.IntegerType, new MetadataBuilder().build()),
    recordsProcessed("recordsProcessed",DataTypes.IntegerType, new MetadataBuilder().build()),
    recordsPerSecond("recordsPerSecond",DataTypes.IntegerType, new MetadataBuilder().build()),
    archiveDatabaseRowAvgLatency("archiveDatabaseRowAvgLatency",DataTypes.IntegerType, new MetadataBuilder().build()),
    timestamp("timestamp",DataTypes.IntegerType, new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());
    private final String label;
    private final DataType type;
    private final Metadata metadata;
    private DPLMetrics(final String label, final DataType type, final Metadata metadata){
        this.label = label;
        this.type = type;
        this.metadata = metadata;
    }

    public String label(){
        return label;
    }

    public DataType type(){
        return type;
    }

    public Metadata metadata(){return metadata;}

    public static StructType schema(){
        final List<StructField> structFields = new ArrayList<StructField>();
        for (final DPLMetrics metric: DPLMetrics.values()) {
            structFields.add(DataTypes.createStructField(metric.label(),metric.type(),false, metric.metadata()));
        }
        return DataTypes.createStructType(structFields.toArray(new StructField[]{}));
    }
}
