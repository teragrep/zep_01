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

package com.teragrep.pth_07;

import com.teragrep.pth_07.stream.BatchHandler;
import com.teragrep.pth_07.ui.UserInterfaceManager;
import com.teragrep.pth_07.ui.elements.table_dynamic.DTTableDatasetNg;
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.DataTablesFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.UPlotFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DataTablesFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DatasetFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UPlotFormat;
import com.teragrep.pth_15.DPLExecutor;
import com.teragrep.pth_15.DPLExecutorFactory;
import com.teragrep.pth_15.DPLExecutorResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.json.JsonObject;
import org.apache.spark.SparkContext;
import com.teragrep.zep_01.interpreter.*;
import com.teragrep.zep_01.interpreter.InterpreterResult.Code;
import com.teragrep.zep_01.interpreter.thrift.InterpreterCompletion;
import com.teragrep.zep_01.scheduler.Scheduler;
import com.teragrep.zep_01.scheduler.SchedulerFactory;
import com.teragrep.zep_01.spark.SparkInterpreter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DPL-Spark SQL interpreter for Zeppelin.
 */
public class DPLInterpreter extends AbstractInterpreter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLInterpreter.class);

    private static long runIncrement = 0L;
    private static final AtomicInteger SESSION_NUM = new AtomicInteger(0);
    private SparkInterpreter sparkInterpreter;
    private SparkContext sparkContext;

    private final DPLExecutor dplExecutor;

    // Parameter handling
    private final Config config;

    private final DPLKryo dplKryo;

    private final HashMap<String, HashMap<String, UserInterfaceManager>> notebookParagraphUserInterfaceManager;


    public DPLInterpreter(final Properties properties) {
        super(properties);
        config = ConfigFactory.parseProperties(properties);
        try {
            dplExecutor = new DPLExecutorFactory("com.teragrep.pth_10.executor.DPLExecutorImpl", config).create();
        } catch (final ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                       IllegalAccessException e) {
            throw new RuntimeException("Error initializing DPLExecutor implementation", e);
        }
        dplKryo = new DPLKryo();
        LOGGER.info("DPL-interpreter initialize properties: {}", properties);
        notebookParagraphUserInterfaceManager = new HashMap<>();
    }

    @Override
    public void open() throws InterpreterException {
        LOGGER.info("DPL-interpreter Open(): {}", properties);

        sparkInterpreter = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class, true);

        if (sparkInterpreter == null) {
            throw new InterpreterException("Could not find open SparkInterpreter in the same interpreter group from session by class name");
        }

        sparkContext = sparkInterpreter.getSparkContext();

        // increase open counter
        SESSION_NUM.incrementAndGet();
        LOGGER.debug("Current session count after opening: {}", SESSION_NUM);

    }

    @Override
    public void close() throws InterpreterException {
        LOGGER.info("Close DPLInterpreter called");
        LOGGER.debug("Current session count before closing: {}", SESSION_NUM);
        SESSION_NUM.decrementAndGet();
        if (dplExecutor != null) {
            LOGGER.info("Closing dplExecutor");
            try {
                dplExecutor.stop();
            } catch (final TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        if (sparkInterpreter != null) {
            LOGGER.info("Closing sparkInterpreter");
            sparkInterpreter.close();
        }
        LOGGER.debug("Current session count after closing: {}", SESSION_NUM);
    }

    @Override
    public ZeppelinContext getZeppelinContext() {
        return sparkInterpreter.getZeppelinContext();
    }

    @Override
    public InterpreterResult internalInterpret(final String lines, final InterpreterContext interpreterContext)
            throws InterpreterException {

        // clear old output
        interpreterContext.out.clear();
        // FIXME clear fron NgDPLRenderer too

        // setup UserInterfaceManager
        final UserInterfaceManager userInterfaceManager = new UserInterfaceManager(interpreterContext);

        // store UserInterfaceManager
        if (!notebookParagraphUserInterfaceManager.containsKey(interpreterContext.getNoteId())) {
            // notebookId does not exist
            final HashMap<String, UserInterfaceManager> paragraphUserInterfaceManager = new HashMap<>();
            notebookParagraphUserInterfaceManager.put(interpreterContext.getNoteId(), paragraphUserInterfaceManager);
        }

        // update the userInterfaceManager
        notebookParagraphUserInterfaceManager.get(interpreterContext.getNoteId()).put(interpreterContext.getParagraphId(), userInterfaceManager);

        // setup batchHandler
        final BatchHandler batchHandler = new BatchHandler(
                userInterfaceManager,
                getZeppelinContext()
        );

        final String jobGroup = Utils.buildJobGroupId(interpreterContext);
        final String jobDesc = Utils.buildJobDesc(interpreterContext);
        sparkContext.setJobGroup(jobGroup, jobDesc, false);

        LOGGER.info("DPL-interpreter jobGroup={}-{}", jobGroup, jobDesc);
        sparkContext.setLocalProperty("spark.scheduler.pool", interpreterContext.getLocalProperties().get("pool"));

        if (sparkInterpreter.isUnsupportedSparkVersion()) {
            return new InterpreterResult(Code.ERROR,
                    "Spark " + sparkInterpreter.getSparkVersion().toString() + " is not supported");
        }

        LOGGER.info("DPL-interpreter interpret incoming string: {}", lines);

        if(lines == null || lines.isEmpty() || lines.trim().isEmpty() ){
            return new InterpreterResult(Code.SUCCESS);
        }

        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

        if (!sparkInterpreter.isScala212()) {
            // TODO(zjffdu) scala 2.12 still doesn't work for codegen (ZEPPELIN-4627)
            Thread.currentThread().setContextClassLoader(sparkInterpreter.getScalaShellClassLoader());
        }

        // execute query
        final InterpreterResult output;
        final SparkSession sparkSession = sparkInterpreter.getSparkSession();
        final String appId = sparkSession.sparkContext().applicationId();
        final String queryId = appId + "-" + runIncrement++;

        final String resultOutput = "Application ID: " + appId +  " , Query ID: " + queryId;
        userInterfaceManager.getMessageLog().addMessage(resultOutput);

        sparkSession.streams().addListener(new DPLMetricsListener(sparkSession, userInterfaceManager, queryId));
        try {
            final DPLExecutorResult executorResult = dplExecutor.interpret(
                    batchHandler,
                    sparkSession,
                    queryId,
                    interpreterContext.getNoteId(),
                    interpreterContext.getParagraphId(),
                    lines
            );
            final InterpreterResult.Code code;
            if (executorResult.code().equals(DPLExecutorResult.Code.SUCCESS)) {
                code = Code.SUCCESS;
            } else if (executorResult.code().equals(DPLExecutorResult.Code.INCOMPLETE)) {
                code = Code.INCOMPLETE;
            } else if (executorResult.code().equals(DPLExecutorResult.Code.KEEP_PREVIOUS_RESULT)) {
                code = Code.KEEP_PREVIOUS_RESULT;
            } else {
                code = Code.ERROR;
            }

            output = new InterpreterResult(code, executorResult.message());
            LOGGER.info("Query done, return code: {}", output.code());
        } catch (final TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (!sparkInterpreter.isScala212()) {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }

        return output;
    }

    @Override
    protected boolean isInterpolate() {
        return true;
    }

    @Override
    public void cancel(final InterpreterContext context) throws InterpreterException {
        LOGGER.info("CANCEL job id: {}", Utils.buildJobGroupId(context));
        LOGGER.debug("Current session count before canceling: {}", SESSION_NUM);
        // Stop streaming after current batch
        if (dplExecutor != null) {
            try {
                dplExecutor.stop();
            } catch (final TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        sparkContext.cancelJobGroup(Utils.buildJobGroupId(context));
        sparkInterpreter.cancel(context);
        LOGGER.debug("Current session count after canceling: {}", SESSION_NUM);
    }


    @Override
    public FormType getFormType() {
        return FormType.NATIVE;
    }

    @Override
    public int getProgress(final InterpreterContext context) throws InterpreterException {
        if (sparkInterpreter != null) {
            return sparkInterpreter.getProgress(context);
        } else {
            return 0;
        }
    }

    @Override
    public Scheduler getScheduler() {
        return SchedulerFactory.singleton().createOrGetFIFOScheduler(
                DPLInterpreter.class.getName() + this.hashCode());
    }

    @Override
    public List<InterpreterCompletion> completion(final String buf, final int cursor, final InterpreterContext interpreterContext) {
        return null;
    }

    private UserInterfaceManager findUserInterfacemanger(final String noteId, final String paragraphId) throws InterpreterException{
        if(notebookParagraphUserInterfaceManager == null){
            throw new InterpreterException("DPLInterpreter's notebookParagraphUserInterfaceManager map is not instantiated!");
        }
        final Map<String,UserInterfaceManager> notebookUserInterfaceManagers = notebookParagraphUserInterfaceManager.get(noteId);
        if(notebookUserInterfaceManagers == null){
            throw new InterpreterException("DPLInterpreter does not have a UserInterfaceManager for note id "+noteId);
        }
        final UserInterfaceManager userInterfaceManager = notebookUserInterfaceManagers.get(paragraphId);
        if(userInterfaceManager == null){
            throw new InterpreterException("DPLInterpreter does not have a UserInterfaceManager for paragraph id "+paragraphId+" within note id "+noteId);
        }
        return userInterfaceManager;
    }

    @Override
    public String formatDataset(final String noteId, final String paragraphId, final String visualizationLibraryName, final Map<String, String> options) throws InterpreterException{
        final UserInterfaceManager userInterfaceManager = findUserInterfacemanger(noteId,paragraphId);

        final DatasetFormat format;
        if(visualizationLibraryName.equals(InterpreterResult.Type.UPLOT.label)){
            final DTTableDatasetNg dtTableDatasetNg = userInterfaceManager.getDtTableDatasetNg();
            final Dataset<Row> dataset = dtTableDatasetNg.dataset();

            final UPlotFormatOptions uplotOptions = new UPlotFormatOptions(options);
            format = new UPlotFormat(dataset, uplotOptions);
        }
        else {
            // Default to DataTables
            final Dataset<Row> dataset = userInterfaceManager.getDtTableDatasetNg().dataset();

            final DataTablesFormatOptions datatablesOptions = new DataTablesFormatOptions(options);
            format = new DataTablesFormat(dataset, datatablesOptions);
        }
        return format.format().toString();
    }
}
