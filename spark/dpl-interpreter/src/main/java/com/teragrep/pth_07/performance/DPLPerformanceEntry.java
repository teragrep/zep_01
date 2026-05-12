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
import com.teragrep.zep_01.common.exception.IncompatibleValueException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public final class DPLPerformanceEntry {
    private final PerformanceSchema performanceSchema;
    private final Map<String,PerformanceMetric<?>> metrics;

    public DPLPerformanceEntry(){
        this(new PerformanceSchema(),new HashMap<>());
    }

    public DPLPerformanceEntry(final PerformanceSchema performanceSchema, final Map<String,PerformanceMetric<?>> metrics){
        this.performanceSchema = performanceSchema;
        this.metrics = metrics;
    }

    public DPLPerformanceEntry withData(final String key, final Object value) throws IncompatibleValueException{
        final Map<String, PerformanceMetric<?>> modifiedMetrics = new HashMap<>(metrics);
        for (final PerformanceMetric<?> metric : performanceSchema.metrics()) {
            if(key.equals(metric.name()+": "+metric.description())){
                final PerformanceMetric<?> modifiedMetric = metric.withValue(value);
                modifiedMetrics.put(metric.name(), modifiedMetric);
                break;
            }
        }
        return new DPLPerformanceEntry(performanceSchema, modifiedMetrics);
    }

    public Row asRow(){
        return asRow(performanceSchema.sparkSchema());
    }
    public Row asRow(final StructType schema){
        final List<Object> values = new ArrayList<>();
        for (final StructField field : schema.fields()) {
            if(metrics.containsKey(field.name())){
                final PerformanceMetric<?> metric = metrics.get(field.name());
                final Object value = metric.value();
                values.add(value);
            }
            else {
                values.add(null);
            }
        }
        return new GenericRowWithSchema(values.toArray(),schema);
    }

    public PerformanceSchema performanceSchema(){
        return performanceSchema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DPLPerformanceEntry entry = (DPLPerformanceEntry) o;
        return Objects.equals(performanceSchema, entry.performanceSchema) && Objects.equals(metrics, entry.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(performanceSchema, metrics);
    }
}
