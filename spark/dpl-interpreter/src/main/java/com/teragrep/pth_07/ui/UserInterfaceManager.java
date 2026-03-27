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
package com.teragrep.pth_07.ui;

import com.teragrep.pth_07.ui.elements.MessageLog;
import com.teragrep.pth_07.ui.elements.PerformanceIndicator;
import com.teragrep.pth_07.ui.elements.table_dynamic.DatasetState;
import com.teragrep.pth_07.ui.elements.table_dynamic.StubDatasetState;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.thrift.Options;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Holds the state of the Dataset, PerformanceIndicator and MessageLog of a single Paragraph.
 * Responsible for passing new Datasets and formatting requests to DatasetState and triggering the writing of the DatasetState to InterpreterOutput for saving on disk and updating UI.
 */
public final class UserInterfaceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserInterfaceManager.class);
    private final AtomicReference<DatasetState> datasetState;
    private final PerformanceIndicator performanceIndicator;
    private final MessageLog messageLog;
    public UserInterfaceManager(final InterpreterContext interpreterContext) {
        this(new AtomicReference<>(new StubDatasetState(interpreterContext.out())), new PerformanceIndicator(interpreterContext), new MessageLog(interpreterContext));
    }

    public UserInterfaceManager(final AtomicReference<DatasetState> datasetState, final PerformanceIndicator performanceIndicator, final MessageLog messageLog){
        this.datasetState = datasetState;
        this.performanceIndicator = performanceIndicator;
        this.messageLog = messageLog;
    }

    public PerformanceIndicator getPerformanceIndicator() {
        return performanceIndicator;
    }

    public MessageLog getMessageLog() {
        return messageLog;
    }

    /**
     * Updates DatasetState to use a new Dataset, and writes the output to InterpreterOutput.
     * @param dataset The dataset containing updated data
     * @throws InterpreterException Thrown if trying to write a StubDatasetState
     */
    public void updateDataset(final Dataset<Row> dataset) throws InterpreterException {
        datasetState.set(datasetState.get().withDataset(dataset));
        datasetState.get().writeDataUpdate();
    }


    /**
     * Formats the current dataset with given formatting options. Used when UI requests the current dataset in a different format.
     * @param options Thrift union object containing the wanted formatting options.
     * @return String representing the formatted dataset.
     * @throws InterpreterException Thrown if an error occurs during formatting of data.
     */
    public String formatDataset(final Options options) throws InterpreterException{
            final JsonObject formattedDataset = datasetState.get().formatDataset(options);
            return formattedDataset.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UserInterfaceManager that = (UserInterfaceManager) o;
        return Objects.equals(datasetState, that.datasetState) && Objects.equals(performanceIndicator, that.performanceIndicator) && Objects.equals(messageLog, that.messageLog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetState, performanceIndicator, messageLog);
    }
}
