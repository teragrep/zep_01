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
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Snapshot of a paragraph's Dataset, its selected format and formatting options. Can switch between formatting options and new Datasets by creating a modified copy of itself.
 * Responsible for writing a formatted representation of the dataset in some DatasetFormat to it's defined InterpreterOutput.
 */
public final class MaterializedDatasetState implements DatasetState {
    private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedDatasetState.class);
    private final Dataset<Row> dataset;
    private final DataTablesFormat dataTablesFormat;
    private final UPlotFormat uPlotFormat;
    private final InterpreterOutput output;
    private final ReentrantLock lock;

    public MaterializedDatasetState(final Dataset<Row> dataset, final InterpreterOutput output, final DataTablesFormat dataTablesFormat, final UPlotFormat uPlotFormat){
        this.dataset = dataset;
        this.output = output;
        this.dataTablesFormat = dataTablesFormat;
        this.uPlotFormat = uPlotFormat;
        this.lock = new ReentrantLock();
    }


    /**
     * Creates a new DatasetState object with an updated Dataset. Unpersists the previous dataset and persists the newly given dataset into memory.
     * Notifies all supported format objects to perform all the operations they require when a new Dataset is received.
     * This method is called when a new batch of data is received from a DPL query.
     * @param rowDataset The updated Dataset
     * @return A new instance of DatasetState, containing the updated Dataset as well as previously existing Options.
     */
    @Override
    public MaterializedDatasetState withDataset(final Dataset<Row> rowDataset){
        Dataset<Row> updatedDataset = dataset;
        // needs to be here as sparkContext might disappear later
        if (rowDataset.schema().nonEmpty()) {
            // unpersist existing dataset upon receiving new data and persist the new dataset.
            updatedDataset.unpersist();
            updatedDataset = rowDataset.persist(StorageLevel.MEMORY_AND_DISK());
        }
        final DataTablesFormat newDataTablesFormat = dataTablesFormat.withDataset(updatedDataset);
        final UPlotFormat newUPlotFormat = uPlotFormat.withDataset(updatedDataset);
        return new MaterializedDatasetState(updatedDataset,output,newDataTablesFormat,newUPlotFormat);
    }


    /**
     * Creates a new DatasetState with an updated DatasetFormat corresponding to the type of Options received.
     * @param options A Thrift union object containing a supported formatting options object
     * @return A new DatasetState instance with updated Options based on the options object provided. If given Options require a different instance of DatasetFormat, it will also be created.
     * @throws InterpreterException When an unsupported Options object is provided
     */
    @Override
    public JsonObject formatDataset(final Options options) throws InterpreterException {
        final JsonObject formatted;
        if(options.isSetDataTablesOptions()){
            formatted = dataTablesFormat.format(options.getDataTablesOptions());
        }
        else if (options.isSetUPlotOptions()){
            formatted = uPlotFormat.format(options.getUPlotOptions());
        }
        else {
            throw new InterpreterException("Unsupported options type!");
        }
        return formatted;
    }

    /**
     * Writes the given String to the InterpreterOutput.
     * @param outputContent String to write
     */

    private void write(final String outputContent){
        try {
            lock.lock();
            output.clear(false);
            output.write(outputContent);
            output.flush();
        } catch (final IOException e) {
            LOGGER.error(e.toString());
            e.printStackTrace();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * Format the current Dataset with the default DataTablesFormat, then write the output to InterpreterOutput.
     * Writing data to InterpreterOutput also saves the output to disk.
     */
    @Override
     public void writeDataUpdate() {
         final DataTablesOptions defaultOptions = new DataTablesOptions(0,0,0,new DataTablesSearch("",false,new ArrayList<>()),new ArrayList<>(),new ArrayList<>());
         final JsonObject formatted = dataTablesFormat.format(defaultOptions);
         final String outputContent = "%"+dataTablesFormat.type().toLowerCase()+"\n" + formatted.toString();
         write(outputContent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaterializedDatasetState that = (MaterializedDatasetState) o;
        return Objects.equals(dataset, that.dataset) && Objects.equals(dataTablesFormat, that.dataTablesFormat) && Objects.equals(uPlotFormat, that.uPlotFormat) && Objects.equals(output, that.output) && Objects.equals(lock, that.lock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataset, dataTablesFormat, uPlotFormat, output, lock);
    }
}
