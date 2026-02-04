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

import com.teragrep.zep_01.interpreter.InterpreterException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UPlotFormatOptions implements FormatOptions{
    private final Map<String, String> optionsMap;
    private final String query;

    public UPlotFormatOptions(Map<String, String> optionsMap, String query){
        this.optionsMap = optionsMap;
        this.query = query;
    }

    public String graphType() throws InterpreterException {
        if(!optionsMap.containsKey("graphType")){
            throw new InterpreterException("Options map does not contain a graphType value");
        }
        return optionsMap.get("graphType");
    }

    public String query() throws InterpreterException {
        if(query.isEmpty()){
            throw new InterpreterException("Query is empty!");
        }
        return query;
    }

    // Gets series labels by finding last instance of " by {seriesName} {seriesName...}|" from query string.
    public List<String> seriesLabels() throws InterpreterException{
        List<String> seriesLabels = new ArrayList<>();
        String query = query();

        Pattern pattern1 = Pattern.compile("(stats.*|timechart.*|eventstats.*|chart.*)");
        Matcher matcher1 = pattern1.matcher(query);

        String lastAggregationQuery = "";
        while (matcher1.find()){
            lastAggregationQuery = matcher1.group();
        }
        if(lastAggregationQuery.startsWith("timechart")){
            seriesLabels.add("_time");
        }

        if(matcher1.groupCount() != 0){
            Pattern pattern = Pattern.compile(" by ([^|\n$]+)");
            Matcher matcher = pattern.matcher(lastAggregationQuery);
            while(matcher.find()){
                List<String> labels = Arrays.asList(matcher.group(matcher.groupCount()).split(" "));
                for (String label: labels) {
                    seriesLabels.add(label);
                }
            }
        }
        return seriesLabels;
    }
}
