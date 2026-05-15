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

import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DataTablesDataTest {
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    @Test
    void asJson() {
        final int draw = 0;
        final int start = 0;
        final int length = 2;
        final String search = "";

        final List<Row> rowList = new ArrayList<>();


        final StructType schema = new StructType()
                .add(new StructField("column1", DataTypes.IntegerType, false, Metadata.empty()))
                .add(new StructField("column2", DataTypes.IntegerType, false, Metadata.empty()));
        rowList.add(RowFactory.create(1,2));
        rowList.add(RowFactory.create(3,4));
        rowList.add(RowFactory.create(5,6));
        final Dataset<Row> sourceData = sparkSession.createDataFrame(rowList,schema);

        final List<String> rows = sourceData.toJSON().collectAsList();

        final int recordsTotal = 3;
        final int recordsFiltered = 3;

        final DataTablesData dataTablesData = new DataTablesData(rows, draw, start, length, search);

        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(Json.createObjectBuilder().add("column1",1).add("column2",2))
                        .add(Json.createObjectBuilder().add("column1",3).add("column2",4))
                        .build())
                .add("draw",draw)
                .add("recordsTotal",recordsTotal)
                .add("recordsFiltered",recordsFiltered)
                .build();

        Assertions.assertEquals(expectedJson,dataTablesData.asJson());
    }

    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(DataTablesData.class)
                .verify();
    }
}