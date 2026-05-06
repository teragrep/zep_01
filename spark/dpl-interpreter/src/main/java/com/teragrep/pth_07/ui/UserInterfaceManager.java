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
import com.teragrep.pth_07.ui.elements.table_dynamic.DatasetStore;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.AvailableFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.RenderFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UIOption;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UIOptionImpl;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;


/**
 * Holds the state of the Dataset, PerformanceIndicator and MessageLog of a single Paragraph.
 * Responsible for passing new Datasets and formatting requests to DatasetState and triggering the writing of the DatasetState to InterpreterOutput for saving on disk and updating UI.
 */
public final class UserInterfaceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserInterfaceManager.class);
    private final DatasetStore datasetStore;
    private final PerformanceIndicator performanceIndicator;
    private final MessageLog messageLog;

    public UserInterfaceManager(final InterpreterContext interpreterContext, Dataset<Row> emptyDataset, UIOption defaultUIOption,List<AvailableFormat> availableFormats) {
        this(new DatasetStore(emptyDataset, availableFormats, interpreterContext, defaultUIOption), new PerformanceIndicator(interpreterContext), new MessageLog(interpreterContext));
    }

    public UserInterfaceManager(final DatasetStore datasetStore, final PerformanceIndicator performanceIndicator, final MessageLog messageLog){
        this.datasetStore = datasetStore;
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
        datasetStore.updateDataset(dataset);
    }


    /**
     * Formats the current dataset with given formatting options. Used when UI requests the current dataset in a different format.
     *
     * @param uiOption UIOption containing the wanted formatting options
     * @return String representing the formatted dataset.
     */
    public String formatDataset(UIOption uiOption){
        RenderFormat renderFormat = datasetStore.toUI(uiOption);
        return renderFormat.toJson().toString();
    }

    public void updateUIOption(UIOption uiOption){
        datasetStore.updateUIOptions(uiOption);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UserInterfaceManager that = (UserInterfaceManager) o;
        return Objects.equals(datasetStore != null ? datasetStore : null, that.datasetStore != null ? that.datasetStore : null)
                && Objects.equals(performanceIndicator, that.performanceIndicator)
                && Objects.equals(messageLog, that.messageLog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetStore != null ? datasetStore : null, performanceIndicator, messageLog);
    }
}
