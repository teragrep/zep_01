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

import com.teragrep.pth_07.performance.metric.value.MetricValue;
import com.teragrep.pth_07.performance.metric.value.MetricValueImpl;
import com.teragrep.zep_01.common.exception.IncompatibleValueException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.util.Objects;

public final class PerformanceMetric<T> {

    private final MetricValue<T> value;
    private final String name;
    private final String description;
    private final DataType type;
    private final Metadata metadata;
    private final boolean nullable;

    public PerformanceMetric(final MetricValue<T> value, final String name, final String description, final DataType type, final Metadata metadata, final boolean nullable){
        this.value = value;
        this.name = name;
        this.description = description;
        this.type = type;
        this.metadata = metadata;
        this.nullable = nullable;
    }

    public MetricValue<T> metricValue() {
        return value;
    }

    public T value(){
        return metricValue().value();
    }

    public PerformanceMetric withValue(final Object value) throws IncompatibleValueException {
        // Check that received object matches with data type of this metric.
        final PerformanceMetric updatedMetric;
        if(type.equals(DataTypes.LongType)){
            final long longValue;
            if(value instanceof Long){
                longValue = (Long) value;
            }
            else if(value instanceof Integer){
                longValue = ((Integer) value).longValue();
            }
            else if(value instanceof String){
                try{
                    longValue = Long.parseLong((String) value);
                }
                catch (final NumberFormatException numberFormatException){
                    throw new IncompatibleValueException("Failed to set metric value! PerformanceMetric "+name+" was unable to parse String data into type "+type.typeName(),numberFormatException);
                }
            }
            else {
                throw new IncompatibleValueException("Failed to set metric value! PerformanceMetric "+name+" expects data of type "+type.typeName()+" but encountered "+value.getClass().getName());
            }
            updatedMetric = new PerformanceMetric<Long>(new MetricValueImpl<Long>(longValue),name,description,type,metadata,nullable);
        }

        else if(type.equals(DataTypes.DoubleType)){
            final double doubleValue;
            if(value instanceof Double){
                doubleValue = (Double) value;
            }
            else if(value instanceof Integer){
                doubleValue = ((Integer) value).doubleValue();
            }
            else if(value instanceof Long){
                doubleValue = ((Long) value).doubleValue();
            }
            else if(value instanceof String){
                try{
                    doubleValue = Double.parseDouble((String) value);
                }
                catch (final NumberFormatException numberFormatException){
                    throw new IncompatibleValueException("Failed to set metric value! PerformanceMetric "+name+" was unable to parse String data into type "+type.typeName(),numberFormatException);
                }
            }
            else {
                throw new IncompatibleValueException("Failed to set metric value! PerformanceMetric "+name+" expects data of type "+type.typeName()+" but encountered "+value.getClass().getName());
            }
            updatedMetric = new PerformanceMetric<Double>(new MetricValueImpl<Double>(doubleValue),name,description,type,metadata,nullable);
        }

        else {
            throw new IncompatibleValueException("Failed to set metric value! PerformanceMetric "+name+" uses unsupported data type "+type.typeName());
        }
        return updatedMetric;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public StructField toStructField() {
        final StructField structField = new StructField(name,type,nullable,metadata);
        return structField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PerformanceMetric<?> metric = (PerformanceMetric<?>) o;
        return nullable == metric.nullable && Objects.equals(value, metric.value) && Objects.equals(name, metric.name) && Objects.equals(description, metric.description) && Objects.equals(type, metric.type) && Objects.equals(metadata, metric.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, name, description, type, metadata, nullable);
    }

}
