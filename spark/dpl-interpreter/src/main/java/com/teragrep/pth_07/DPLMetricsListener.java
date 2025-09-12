package com.teragrep.pth_07;

import com.teragrep.pth_07.ui.UserInterfaceManager;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

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
        }
    }

    @Override
    public void onQueryTerminated(final QueryTerminatedEvent event) {
        sparkSession.streams().removeListener(this);
    }
}
