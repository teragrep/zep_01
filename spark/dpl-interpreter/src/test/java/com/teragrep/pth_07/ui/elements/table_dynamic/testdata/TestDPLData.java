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
package com.teragrep.pth_07.ui.elements.table_dynamic.testdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDPLData {
        private final SparkSession sparkSession;
        private final StructType schema;

        public TestDPLData(SparkSession sparkSession, StructType schema){
            this.sparkSession = sparkSession;
            this.schema = schema;
        }

        /**
         * Tries to generate a dataset of size 'amount' with default values corresponding to varargs 'values'
         * @param amount - desired number of rows in the dataset
         * @param values - varargs specifying the default values to fill into the dataset.
         * @return
         */

        public Dataset<Row> createDataset(int amount, Object ... values){
            final List<Row> rows = rowList(amount,values);
            return sparkSession.createDataFrame(rows, schema);
        }

        /**
         * Generates a List of Rows based on given values.
         * @param amount - Number of rows to generate
         * @param values - Default values to add for each Row. If first value given is a Timestamp, it will be incremented by one for each Row.
         * @return
         */
        private List<Row> rowList(int amount, Object ... values){
            final List<Object> valueList = Arrays.asList(values);
            final ArrayList<Row> rowArrayList = new ArrayList<>();
            while (amount > 0) {
                if(valueList.get(0) instanceof Timestamp){
                    valueList.set(0, Timestamp.from(Instant.ofEpochSecond(amount)));
                }
                final Row row = RowFactory.create(valueList.toArray());
                rowArrayList.add(row);
                amount--;
            }
            return rowArrayList;
        }
    }