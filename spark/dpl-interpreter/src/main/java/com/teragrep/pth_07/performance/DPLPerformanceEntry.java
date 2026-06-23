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
import com.teragrep.pth_07.performance.metric.value.MetricValue;
import com.teragrep.zep_01.common.exception.IncompatibleValueException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class DPLPerformanceEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLPerformanceEntry.class);
    private final Map<String,PerformanceMetric> metrics;

    /**
     * DPLPerformanceEntry represents a row of performance data from a DPL query. Each batch received from DPL corresponds to one DPLPerformanceEntry.
     * DPLPerformanceEntry keeps a map of PerformanceMetrics, each of which can be updated using the withData() methods.
     * DPLPerformanceEntry can produce a Spark schema that contains all the information for each of the PerformanceMetrics it contains.
     * DPLPerformanceEntry is capable of turning itself into a Spark Row that can be added to a Spark Dataset.
     */

    /**
     * Create a PerformanceEntry containing Stub metrics according to the default schema.
     */
    public DPLPerformanceEntry(MetricsSchema schema){
        this(schema.metricsAsMap());
    }

    /**
     * Create a PerformanceSchema containing the provided metrics.
      */
    public DPLPerformanceEntry(final Map<String,PerformanceMetric> metrics){
        this.metrics = metrics;
    }

    /**
     * Creates a new instance of this entry, with an updated value for the metric identified by key. If given key does not match with any of the metrics, returns this entry without modification.
     * @param key PerformanceMetric to update. Must match with the key of one of the PerformanceMetrics this object was initialized with.
     * @param value new value
     * @return a modified instance of this DPLPerformanceEntry, with the PerformanceMetric identified by key having it's value replaced by the given value.
     */
    public DPLPerformanceEntry withData(final String key, final long value) {
        DPLPerformanceEntry rv = this;
        if(metrics.containsKey(key)) {
            try {
                PerformanceMetric metric = metrics.get(key);
                PerformanceMetric modifiedMetric = metric.withValue(value);
                Map<String, PerformanceMetric> modifiedMetrics = new HashMap<>(metrics);
                modifiedMetrics.put(key, modifiedMetric);
                rv = new DPLPerformanceEntry(modifiedMetrics);
            } catch (IncompatibleValueException incompatibleValueException) {
                LOGGER.warn("Failed to update Query performance data for metric <[{}]> due to mismatched data type ", key, incompatibleValueException);
                rv = this;
            }
        }
        return rv;
    }

    /**
     * Creates a new instance of this entry, with an updated value for the metric identified by key. If given key does not match with any of the metrics, returns this entry without modification.
     * @param key PerformanceMetric to update. Must match with the key of one of the PerformanceMetrics this object was initialized with.
     * @param value new value
     * @return a modified instance of this DPLPerformanceEntry, with the PerformanceMetric identified by key having it's value replaced by the given value.
     */
    public DPLPerformanceEntry withData(final String key, final double value) {
        DPLPerformanceEntry rv = this;
        if(metrics.containsKey(key)){
            try{
                PerformanceMetric metric = metrics.get(key);
                PerformanceMetric modifiedMetric = metric.withValue(value);
                Map<String, PerformanceMetric> modifiedMetrics = new HashMap<>(metrics);
                modifiedMetrics.put(key,modifiedMetric);
                rv = new DPLPerformanceEntry(modifiedMetrics);
            }
            catch (IncompatibleValueException incompatibleValueException){
                LOGGER.warn("Failed to update Query performance data for metric <[{}]> due to mismatched data type ",key,incompatibleValueException);
                rv = this;
            }
        }
        return rv;
    }

    /**
     * Turns the PerformanceMetrics within this entry into a Spark Row.
     * Any PerformanceMetrics containing a StubMetricValue will be represented as a null.
     * The schema of the row corresponds to the given metrics.
     * @return Spark Row object representing this entry
     */
    public Row asRow(){
        return asRow(performanceSchema());
    }

    /**
     * Turns the PerformanceMetrics within this entry that match with the given Schema into a Spark Row.
     * Any PerformanceMetrics containing a StubMetricValue will be represented as a null
     * Any PerformanceMetrics that do not match with the given Schema will be skipped
     * Any Schema fields that don't have a matching PerformanceMetric will be represented as a null
     * @param schema Schema to apply to the Row
     * @return Spark Row object representing this entry
     */
    public Row asRow(final StructType schema){
        final List<Object> values = new ArrayList<>();
        for (final StructField field : schema.fields()) {
            if(metrics.containsKey(field.name()) && !metrics.get(field.name()).value().isStub()){
                final MetricValue metricValue = metrics.get(field.name()).value();
                values.add(metricValue.value());
            }
            else {
                values.add(null);
            }
        }
        return new GenericRowWithSchema(values.toArray(),schema);
    }

    /**
     * Generates a Spark Schema based on the PerformanceMetrics this object was initialized with
     * @return Spark Schema containing information on the metrics defined in this entry
     */
    public StructType performanceSchema(){
        StructType performanceSchema = new StructType();
        for (PerformanceMetric metric : metrics.values()) {
            performanceSchema = performanceSchema.add(metric.toStructField());
        }
        return performanceSchema;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DPLPerformanceEntry entry = (DPLPerformanceEntry) o;
        return Objects.equals(metrics, entry.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics);
    }
}
