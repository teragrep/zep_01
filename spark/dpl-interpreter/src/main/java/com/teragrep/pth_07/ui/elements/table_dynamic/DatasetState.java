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
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UPlotFormat;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.InterpreterResult;
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

/**
 * Snapshot of a paragraph's Dataset, its selected format and formatting options. Can switch between formatting options and new Datasets by creating a modified copy of itself.
 * Responsible for writing a formatted representation of the dataset in some DatasetFormat to it's defined InterpreterOutput.
 */
public final class DatasetState {
    private final Dataset<Row> dataset;
    private final DatasetFormat format;
    private final Options options;
    private final InterpreterOutput output;
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetState.class);

    // Constructor for initializing an empty DatasetState. Null is used to represent absence of Dataset, as creating an empty dataset requires a SparkSession and would cause overhead. Stubbing is also infeasible due to Dataset not implementing any interface
    public DatasetState(final InterpreterOutput output){
        this(null, output,new DataTablesFormat(), Options.dataTablesOptions(new DataTablesOptions(0,0,50,new DataTablesSearch("",false,new ArrayList<>()),new ArrayList<>(),new ArrayList<>())));
    }

    public DatasetState(final Dataset<Row> dataset, final InterpreterOutput output, final DatasetFormat format, Options options){
        this.dataset = dataset;
        this.output = output;
        this.format = format;
        this.options = options;
    }


    /**
     * Creates a new DatasetState object with an updated Dataset. Unpersists the previous dataset and persists the newly given dataset into memory.
     * @param rowDataset The updated Dataset
     * @return A new instance of DatasetState, containing the updated Dataset as well as previously existing Options.
     */
    public DatasetState withDataset(final Dataset<Row> rowDataset) {
        Dataset<Row> currentDataset = dataset;
        // needs to be here as sparkContext might disappear later
        if (rowDataset.schema().nonEmpty()) {
            // unpersist dataset upon receiving new data
            if(currentDataset != null){
                currentDataset.unpersist();
            }
            currentDataset = rowDataset.persist(StorageLevel.MEMORY_AND_DISK());
        }
        return new DatasetState(currentDataset,output,format,options);
    }


    /**
     * Creates a new DatasetState with an updated DatasetFormat corresponding to the type of Options received.
     * @param options A Thrift union object containing a supported formatting options object
     * @return A new DatasetState instance with updated Options based on the options object provided. If given Options require a different instance of DatasetFormat, it will also be created.
     * @throws InterpreterException When an unsupported Options object is provided
     */
    public DatasetState withOptions(final Options options) throws InterpreterException {
        DatasetFormat newFormat;
        if(options.isSetDataTablesOptions()){
            // If the current format already is of the correct type, we don't need to create a new instance (which would reset DataTablesFormat's draw counter)
            if(format.type().equals(InterpreterResult.Type.DATATABLES.label)){
                newFormat = format;
            }
            // If the format changes, a fresh instance is required.
            else {
                newFormat = new DataTablesFormat();
            }
        }
        else if (options.isSetUPlotOptions()){
            newFormat = new UPlotFormat();
        }
        else {
            throw new InterpreterException("Unsupported options type!");
        }
        return new DatasetState(dataset,output,newFormat,options);
    }

    /**
     * Writes the given String to the InterpreterOutput.
     * @param outputContent String to write
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
     * Format the current Dataset with the current DatasetFormat and formatting options, then write the output to InterpreterOutput.
     * @throws InterpreterException
     */
     public void writeDataUpdate() throws InterpreterException{
        final JsonObject formatted = format.format(dataset,options);
        final String outputContent = "%"+format.type().toLowerCase()+"\n" +
                formatted.toString();
        write(outputContent);
    }
}
