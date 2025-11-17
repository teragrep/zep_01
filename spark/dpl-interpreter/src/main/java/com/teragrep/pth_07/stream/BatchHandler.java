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
package com.teragrep.pth_07.stream;

import com.teragrep.pth_07.ui.UserInterfaceManager;
import com.teragrep.pth_07.ui.elements.table_dynamic.DTTableDatasetNg;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.teragrep.zep_01.interpreter.ZeppelinContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class BatchHandler implements BiConsumer<Dataset<Row>, Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);

    private final UserInterfaceManager userInterfaceManager;
    private final ZeppelinContext zeppelinContext;
    private final InterpreterContext interpreterContext;
    private final AtomicInteger drawCount;

    public BatchHandler(UserInterfaceManager userInterfaceManager, ZeppelinContext zeppelinContext, InterpreterContext interpreterContext) {
        this(userInterfaceManager,zeppelinContext,interpreterContext,new AtomicInteger(1));
    }
    public BatchHandler(UserInterfaceManager userInterfaceManager, ZeppelinContext zeppelinContext, InterpreterContext interpreterContext, AtomicInteger drawCount) {
        this.userInterfaceManager = userInterfaceManager;
        this.zeppelinContext = zeppelinContext;
        this.interpreterContext = interpreterContext;
        this.drawCount = drawCount;
    }

    @Override
    public void accept(final Dataset<Row> rowDataset, final Boolean aggsUsed) {
        LOGGER.error("BatchHandler accept called LOGGER");
        if (aggsUsed) {
            // need to check aggregatesUsed from visitor at this point, since it can be updated in sequential mode
            // after the parallel operations are performed

            // use legacy table
            userInterfaceManager.getOutputContent().setOutputContent(zeppelinContext.showData(rowDataset));
        }
        else {
            // use DTTableNg
            // If it can be guaranteed that every batch received from a single call to DPLInterpreter.interpret() always has the same schema, we can move incrementing of drawCount to DTTableDatasetNG.drawDataset(), and resetting will occur when a new instance is created.
            if(! userInterfaceManager.getDtTableDatasetNg().isStub()){
                Dataset<Row> oldDataset = userInterfaceManager.getDtTableDatasetNg().getDataset();
                if(oldDataset!=null && oldDataset.schema().equals(rowDataset.schema())){
                    drawCount.incrementAndGet();
                }
                else {
                    drawCount.set(1);
                }
            }
            DTTableDatasetNg dtTableDatasetNg = new DTTableDatasetNg(rowDataset);
            userInterfaceManager.setDtTableDatasetNg(dtTableDatasetNg);
            String outputContent = dtTableDatasetNg.drawDataset(drawCount.get());
            try{
                interpreterContext.out().clear(false);
                interpreterContext.out().write(outputContent);
                interpreterContext.out().flush();
            } catch (IOException ioException){
                LOGGER.error("Failed to write received batch to UI!",ioException);
            }
        }
    }
}
