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
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.DataTablesSearch;
import com.teragrep.zep_01.interpreter.thrift.Options;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DTTableDatasetNG is responsible for writing an output String created from a Dataset to an InterpreterOutput using a given DatasetFormat and Options.
 */
public final class DTTableDatasetNg {
    // FIXME Exceptions should cause interpreter to stop

    private final Lock lock = new ReentrantLock();
    private Dataset<Row> dataset = null; // Using null here to represent absence of a Dataset since creating empty Datasets requires a SparkSession and causes overhead.
    private DatasetFormat previousFormat;
    private Options previousOptions;
    private final InterpreterOutput output;
    private static final Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNg.class);

    public DTTableDatasetNg(final InterpreterOutput output){
        this.output = output;
        this.previousFormat = new DataTablesFormat();
        this.previousOptions = Options.dataTablesOptions(new DataTablesOptions(0,0,50,new DataTablesSearch("",false,new ArrayList<>()),new ArrayList<>(),new ArrayList<>()));
    }

    /**
     * Set a new Dataset which will be used in subsequent formatting requests.
     * The given Dataset will be persisted when this method is called, and any previously set Datasets will be unpersisted.
     * @param rowDataset the new Dataset
     */
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

    /**
     * Writes the given String to the InterpreterOutput within InterpreterContext.
     * @param outputContent Output to write
     */

    private void write(final String outputContent){
        try {
            output.clear(false);
            output.write(outputContent);
            output.flush();
        } catch (final IOException e) {
            LOGGER.error(e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Returns the most recently used DatasetFormat. If no formatting has been done, returns the default DatasetFormat.
     * @return Most recently used DatasetFormat
     */
    public DatasetFormat previousFormat(){
        return previousFormat;
    }

    /**
     * Format the current Dataset with the most recently used DatasetFormat and write the output to InterpreterOutput. If no formatting has been done previously, default formatting will be used.
     * This method can be called repeatedly without overriding previously given formatting options.
     * @throws InterpreterException
     */
    // This method is called when receiving batch updates from BatchHandler. This way if UI requests a different format while a query is running, subsequently received updates from BatchHandler won't override the formatting back to default DataTables.
    public void writeDataUpdate() throws InterpreterException{
        writeDataUpdate(previousFormat, previousOptions);
    }

    /**
     * Format the current Dataset with the given DatasetFormat and Options and write the output to InterpreterOutput.
     * The given DatasetFormat and Options will be stored as the most recently used formatting, and will be used by subsequent calls to writeDataUpdate()
     * @param format DatasetFormat to be used in formatting of the Dataset
     * @param options Options containing parameters needed by the DatasetFormat
     * @throws InterpreterException An error occurred during formatting.
     */
    public void writeDataUpdate(final DatasetFormat format, final Options options) throws InterpreterException{
        try{
            // Calls to this method might come concurrently from both DPLInterpreter (UI requesting a formatting change) and from BatchHandler (receiving a new batch of data from a running query)
            // We acquire the lock here to avoid concurrent calls to write(), as well as concurrent assignments to previousFormat and Options.
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
