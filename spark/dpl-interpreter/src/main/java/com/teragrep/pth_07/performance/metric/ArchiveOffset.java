
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

import com.teragrep.pth_07.performance.metric.value.MetricValueImpl;
import com.teragrep.pth_07.performance.metric.value.MetricValue;
import com.teragrep.pth_07.performance.metric.value.StubMetricValue;
import com.teragrep.zep_01.common.exception.IncompatibleValueException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.util.Objects;

public final class ArchiveOffset implements PerformanceMetric<Long> {
    private final MetricValue<Long> value;
    public ArchiveOffset(){
        this(new StubMetricValue<>());
    }
    public ArchiveOffset(final long value){
        this(new MetricValueImpl<>(value));
    }
    private ArchiveOffset(final MetricValue<Long> value){
        this.value = value;
    }
    public MetricValue<Long> metricValue() {
        return value;
    }
    @Override
    public ArchiveOffset withValue(java.lang.Object value) throws IncompatibleValueException {
        final ArchiveOffset modifiedMetric;
        if(value instanceof String){
            try{
                long newValue = Long.parseLong((String)value);
                modifiedMetric = new ArchiveOffset(newValue);
            }
            catch (NumberFormatException numberFormatException){
                throw new IncompatibleValueException("Value "+value+" is not a compatible value for metric "+name());
            }
        }
        else if(value instanceof Long){
            modifiedMetric = new ArchiveOffset((Long)value);
        }
        else {
            throw new IncompatibleValueException("Value "+value+" is not a compatible value for metric "+name());
        }
        return modifiedMetric;
    }
    public String name() {
        return "ArchiveOffset";
    }

    
    public String description() {
        return "latest archive offset processed (epoch time)";
    }

    
    public DataType type() {
        return DataTypes.LongType;
    }

    
    public Metadata metadata() {
        return Metadata.empty();
    }

    
    public StructField structField(){
        return DataTypes.createStructField(name(),type(),true,metadata());
    }

    
    public boolean equals(final java.lang.Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ArchiveOffset that = (ArchiveOffset) o;
        return Objects.equals(value, that.value);
    }

    
    public int hashCode() {
        return Objects.hash(value);
    }
}