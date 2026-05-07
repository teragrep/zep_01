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

import com.teragrep.pth_07.ui.elements.table_dynamic.formats.*;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class DatasetStore {
    private final AtomicReference<UIOption> uiOptionsRef;
    private final AtomicReference<RenderableDataset> datasetRef;
    private final List<AvailableFormat> availableFormats;
    private final InterpreterContext interpreterContext;

    public DatasetStore(
            Dataset<Row> emptyDataset,
            List<AvailableFormat> availableFormats,
            InterpreterContext interpreterContext,
            UIOption defaultUIOption
    ) {
        this(new AtomicReference<>(new RenderableDatasetImpl(availableFormats, emptyDataset)),new AtomicReference<>(defaultUIOption), availableFormats, interpreterContext);
    }

    private DatasetStore(AtomicReference<RenderableDataset> datasetRef, AtomicReference<UIOption> uiOptionsRef, List<AvailableFormat> availableFormats, InterpreterContext interpreterContext) {
        this.datasetRef = datasetRef;
        this.uiOptionsRef = uiOptionsRef;
        this.availableFormats = availableFormats;
        this.interpreterContext = interpreterContext;
    }

    public void updateDataset(Dataset<Row> rowDataset) throws InterpreterException {
        datasetRef.get().unpersist();
        RenderableDataset rd = new RenderableDatasetImpl(availableFormats, rowDataset);
        datasetRef.set(rd);
        rd.persist();

        RenderFormat uiResponse = datasetRef.get().toRenderFormat(uiOptionsRef.get());
        StringBuilder output = new StringBuilder();
        // Prepend "%{InterpreterResult.Type}\n so that InterpreterOutput.write() parses the type correctly instead of treating the output as TEXT
        output.append("%"+uiResponse.type().label.toLowerCase());
        output.append("\n");
        output.append(uiResponse.toJson().toString());
        try{
            writeToDisk(output.toString());
        }
        catch (IOException e){
            throw new InterpreterException("Failed to write dataset update to disk!",e);
        }
    }

    // Switch output format for subsequent batches
    public void updateUIOptions(UIOption option) {
        uiOptionsRef.set(option);
    }

    // Format current dataset to given format
    public RenderFormat toUI(UIOption uiOption) {
        return datasetRef.get().toRenderFormat(uiOption);
    }

    // Write given String to disk via InterpreterOutput. NotebookServer catches this event in onOutputUpdate() / onOutputAppend()
    private void writeToDisk(String output) throws IOException {
        interpreterContext.out.clear(false);
        interpreterContext.out.write(output);
        interpreterContext.out.flush();
    }
}