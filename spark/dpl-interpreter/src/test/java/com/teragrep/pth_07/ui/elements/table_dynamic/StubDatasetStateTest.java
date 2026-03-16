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
package com.teragrep.pth_07.ui.elements.table_dynamic;

import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;


class StubDatasetStateTest {

    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    // Calling .withDataset(Dataset) on a StubDatasetState should return a MaterializedDatasetState object.
    @Test
    void withDatasetTest() {
        Dataset<Row> emptyData = sparkSession.emptyDataFrame();
        StubDatasetState stubState = new StubDatasetState(new InterpreterOutput());
        DatasetState materializedState = Assertions.assertDoesNotThrow(() -> stubState.withDataset(emptyData));
        Assertions.assertEquals(MaterializedDatasetState.class,materializedState.getClass());
    }

    // Trying to format a StubDatasetState should throw an error.
    @Test
    void formatDatasetTest() {
        StubDatasetState stubState = new StubDatasetState(new InterpreterOutput());
        Options options = Options.dataTablesOptions(new DataTablesOptions());
        Assertions.assertThrows(InterpreterException.class,()->stubState.formatDataset(options));
    }

    // Trying to write to a StubDatasetState's InterpreterOutput should throw an error.
    @Test
    void writeDataUpdateTest() {
        StubDatasetState stubState = new StubDatasetState(new InterpreterOutput());
        Assertions.assertThrows(InterpreterException.class,()->stubState.writeDataUpdate());
    }
}