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
import com.teragrep.zep_01.common.exception.IncompatibleValueException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class PerformanceMetricTest {

    @Test
    void testWithValueLongMetric() throws IncompatibleValueException {
        final PerformanceMetric<Long> metric = new PerformanceMetric(new StubMetricValue(),"testName","desc", DataTypes.LongType, Metadata.empty(),false);

        final PerformanceMetric<Long> newMetric = metric.withValue(-5);
        Assertions.assertEquals(-5,newMetric.metricValue().value());

        final PerformanceMetric<Long> newMetric2 = metric.withValue("-25");
        Assertions.assertEquals(-25,newMetric2.metricValue().value());

        Assertions.assertThrows(IncompatibleValueException.class, ()-> metric.withValue(1.0));
        Assertions.assertThrows(IncompatibleValueException.class, ()-> metric.withValue("one"));
    }

    @Test
    void testWithValueDoubleMetric() throws IncompatibleValueException {
        final PerformanceMetric<Double> metric = new PerformanceMetric(new StubMetricValue(),"testName","desc", DataTypes.DoubleType, Metadata.empty(),false);

        final PerformanceMetric<Double> newMetric = metric.withValue(1.0);
        Assertions.assertEquals(1.0,newMetric.metricValue().value());

        final PerformanceMetric<Double> newMetric2 = metric.withValue("-2.5");
        Assertions.assertEquals(-2.5,newMetric2.metricValue().value());

        final PerformanceMetric<Double> newMetric3 = metric.withValue(-25);
        Assertions.assertEquals(-25.0,newMetric3.metricValue().value());

        Assertions.assertThrows(IncompatibleValueException.class, ()-> metric.withValue("one point five"));
    }

    @Test
    public void testTest(){
        final PerformanceMetric<Integer> metric = new PerformanceMetric<Integer>(new StubMetricValue<Integer>(),"test","desc",DataTypes.IntegerType,Metadata.empty(),true);
        metric.metricValue();
    }
    @Test
    public void testContract() {
        final Metadata redMetaData = Metadata.empty();
        final Metadata blueMetaData = new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build();
        EqualsVerifier.forClass(PerformanceMetric.class)
                .withPrefabValues(Metadata.class,redMetaData, blueMetaData)
                .verify();
    }
}