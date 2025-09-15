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

import com.teragrep.pth_07.ui.UserInterfaceManager;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.Map;

public final class DPLMetricsListener extends StreamingQueryListener {
    private final SparkSession sparkSession;
    private final UserInterfaceManager uiManager;
    private final String queryId;

    public DPLMetricsListener(
            final SparkSession sparkSession,
            final UserInterfaceManager uiManager,
            final String queryId) {
        this.sparkSession = sparkSession;
        this.uiManager = uiManager;
        this.queryId = queryId;
    }

    @Override
    public void onQueryStarted(final QueryStartedEvent event) {
        // no-op
    }

    @Override
    public void onQueryProgress(final QueryProgressEvent event) {
        if (event.progress().name().equals(queryId)) {
            uiManager.getPerformanceIndicator().setPerformanceData(
                    event.progress().numInputRows(),
                    event.progress().batchId(),
                    event.progress().processedRowsPerSecond()
            );

            final Map<String, String> currentMetrics = new HashMap<>();
            final Seq<SQLExecutionUIData> executionsList = sparkSession.sharedState().statusStore().executionsList();
            if (!executionsList.isEmpty()) {
                final Iterator<SQLExecutionUIData> executionDataIterator = sparkSession.sharedState().statusStore().executionsList().iterator();
                while (executionDataIterator.hasNext()) {
                    final SQLExecutionUIData executionData = executionDataIterator.next();
                    final Map<Object, String> metricValues = JavaConverters.mapAsJavaMap(executionData.metricValues());
                    for (final SQLPlanMetric metric : JavaConverters.asJavaIterable(executionData.metrics())) {
                        final long id = metric.accumulatorId();
                        final Object value = metricValues.get(id);
                        if (value != null) {
                            currentMetrics.put(metric.name(), value.toString());
                        }
                    }
                }

                currentMetrics.forEach((key, value) -> {
                    uiManager.getMessageLog().addMessage(key + ": " + value);
                });
            }
        }
    }

    @Override
    public void onQueryTerminated(final QueryTerminatedEvent event) {
        sparkSession.streams().removeListener(this);
    }
}
