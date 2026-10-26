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
package com.teragrep.pth_07.ui.elements.table_dynamic.formats;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public final class DataTablesDatasetTransformation {

    private final Dataset<Row> dataset;
    private final List<Order> order;

    public DataTablesDatasetTransformation(final Dataset<Row> dataset, final List<Order> order) {
        this.dataset = dataset;
        this.order = order;
    }

    public Dataset<Row> order(){
        final Dataset<Row> rv;
        if(order.isEmpty()){
            // No ordering was given, no need to do any transformation
            rv = dataset;
        }
        else {
            String[] colNames = dataset.columns();
            Column[] columns = order.stream().map((order)->{
                final Column column;
                if(order.descending()){
                    column = org.apache.spark.sql.functions.col(colNames[order.columnIndex()]).desc();
                }
                else {
                    column = org.apache.spark.sql.functions.col(colNames[order.columnIndex()]).asc();
                }
                return column;
            }).toArray(Column[]::new);
            rv = dataset.orderBy(columns);
        }
        return rv;
    }


}
