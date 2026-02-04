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
package com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class UPlotFormatOptionsTest {

    // Series names should be parsed from the DPL query, where the last aggregation command used determines the names of the series.
    // In case no aggregation commands are used, the series names should return an empty list.
    // If there are multiple aggregation commands, tha latest one is used.
    // Any other commands have no effect on series names.
    @Test
    public void testSeriesNames(){
        UPlotFormatOptions options = new UPlotFormatOptions(new HashMap<>(),"%dpl\n" +
                "index=crud earliest=-5y\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats sum(countOperation) min(countOperation) max(countOperation) by operation success\n" +
                "| stats operation by operation success _time");
        List<String> seriesNames = Assertions.assertDoesNotThrow(()->options.seriesLabels());
        List<String> expectedNames = new ArrayList<>();
        expectedNames.add("operation");
        expectedNames.add("success");
        expectedNames.add("_time");
        Assertions.assertEquals(expectedNames,seriesNames);
    }

    @Test
    public void testSeriesNamesWithNoAggregation(){
        UPlotFormatOptions options = new UPlotFormatOptions(new HashMap<>(),"%dpl\n" +
                "index=crud earliest=-5y\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats sum(countOperation)");
        List<String> seriesNames = Assertions.assertDoesNotThrow(()->options.seriesLabels());
        List<String> expectedNames = new ArrayList<>();
        Assertions.assertEquals(expectedNames,seriesNames);
    }

    @Test
    public void testSeriesNamesWithNonAggregateLastCommand(){
        UPlotFormatOptions options = new UPlotFormatOptions(new HashMap<>(),"%dpl\n" +
                "index=crud earliest=-5y\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats sum(countOperation) min(countOperation) max(countOperation) by operation success\n" +
                "| accum countOperation AS totalOperationCount");
        List<String> seriesNames = Assertions.assertDoesNotThrow(()->options.seriesLabels());
        List<String> expectedNames = new ArrayList<>();
        expectedNames.add("operation");
        expectedNames.add("success");
        Assertions.assertEquals(expectedNames,seriesNames);
    }


}