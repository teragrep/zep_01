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

import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DataTablesFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UPlotFormat;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.DataTablesSearch;
import com.teragrep.zep_01.interpreter.thrift.Options;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;

public class StubDatasetState implements DatasetState{
    private final InterpreterOutput output;
    public StubDatasetState(InterpreterOutput output){
        this.output = output;
    }

    /**
     * Use this method to turn a StubDatasetState into a MaterializedDatasetState using the given Dataset. Initializes every supported format with the given data
     * @param rowDataset The Dataset to use
     * @return A new MaterializedDatasetState that contains the same InterpreterOutput as this StubDatasetState, the given Dataset.
     */
    @Override
    public DatasetState withDataset(Dataset<Row> rowDataset){
        return new MaterializedDatasetState(rowDataset,output,new DataTablesFormat(rowDataset, rowDataset.toJSON().collectAsList(), 1),new UPlotFormat());
    }

    @Override
    public JsonObject formatDataset(Options options) throws InterpreterException {
        throw new InterpreterException("Attempting to format an empty dataset!");
    }

    @Override
    public void writeDataUpdate() throws InterpreterException {
        throw new InterpreterException("Attempting to write an empty dataset!");
    }
}
