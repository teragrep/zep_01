package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.scheduler.Status;

import java.util.Date;
import java.util.Map;

public interface Job<T> {

    void setId(String id);

    String getId();

    Status getStatus();

    void setStatusWithoutNotification(Status status);

    void setStatus(Status status);

    void setListener(JobListener listener);

    JobListener getListener();

    boolean isTerminated();

    boolean isRunning();

    void onJobStarted();

    void onJobEnded();

    void run();

    Throwable getException();

    T getReturn();

    String getJobName();

    void setJobName(String jobName);

    int progress();

    Map<String, Object> info();

    void abort();

    boolean isAborted();

    Date getDateCreated();

    Date getDateStarted();

    void setDateStarted(Date startedAt);

    Date getDateFinished();

    void setDateFinished(Date finishedAt);

    void setResult(T result);

    String getErrorMessage();

    void setErrorMessage(String errorMessage);

    void setAborted(boolean aborted);

    boolean jobAbort();
}
