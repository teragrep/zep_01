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
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DatasetFormat;
import com.teragrep.pth_07.ui.elements.AbstractUserInterfaceElement;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.DataTablesSearch;
import com.teragrep.zep_01.interpreter.thrift.Options;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class DTTableDatasetNg extends AbstractUserInterfaceElement {
    // FIXME Exceptions should cause interpreter to stop

    private final Lock lock = new ReentrantLock();
    private Dataset<Row> dataset = null; // Using null here to represent absence of a Dataset since creating empty Datasets requires a SparkSession and causes overhead.
    private DatasetFormat previousFormat;
    private Options previousOptions;

    public DTTableDatasetNg(final InterpreterContext interpreterContext){
        super(interpreterContext);
        // Default format is DataTables with page size of 50
        this.previousFormat = new DataTablesFormat();
        this.previousOptions = Options.dataTablesOptions(new DataTablesOptions(0,0,50,new DataTablesSearch("",false,new ArrayList<>()),new ArrayList<>(),new ArrayList<>()));
    }
    @Override
    public void draw() {
    }

    @Override
    public void emit() {
    }

    public void setParagraphDataset(final Dataset<Row> rowDataset) {
        /*
         TODO check if other presentation can be used than string, for order
         i.e. rowDataset.collectAsList()
         */

        try {
            lock.lock();
            // unpersist dataset upon receiving new data
            if(dataset != null){
                dataset.unpersist();
            }
            if (rowDataset.schema().nonEmpty()) {
                // needs to be here as sparkContext might disappear later
                dataset = rowDataset.persist(StorageLevel.MEMORY_AND_DISK());
            }
        } finally {
            lock.unlock();
        }
    }

    private void write(final String outputContent){
        try {
            getInterpreterContext().out().clear(false);
            getInterpreterContext().out().write(outputContent);
            getInterpreterContext().out().flush();
        } catch (final IOException e) {
            LOGGER.error(e.toString());
            e.printStackTrace();
        }
    }
    public DatasetFormat previousFormat(){
        return previousFormat;
    }

    // If given no format or options, default both to whichever was used last.
    // This way if UI requests a different format while a query is running, subsequently received updates from BatchHandler won't override the formatting back to default DataTables.
    public void writeDataUpdate() throws InterpreterException{
        writeDataUpdate(previousFormat, previousOptions);
    }
    public void writeDataUpdate(final DatasetFormat format, Options options) throws InterpreterException{
        try{
            // Calls to this method might come concurrently from both DPLInterpreter (UI requesting a formatting change) and from BatchHandler (receiving a new batch of data from a running query)
            // We acquire the lock here to avoid concurrent calls to write(), as well as concurrent assignments to previousFormat and Options.
            // TODO: perhaps the saving of previously used format and options could be done in an immutable fashion?
            lock.lock();
            final JsonObject formatted = format.format(dataset,options);
            final String outputContent = "%"+format.type().toLowerCase()+"\n" +
                    formatted.toString();
            write(outputContent);
            previousFormat = format;
            previousOptions = options;
        }finally {
            lock.unlock();
        }
    }
}
