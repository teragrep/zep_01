/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.scheduler;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.interpreter.xref.Job;
import org.apache.zeppelin.interpreter.xref.JobListener;
import org.apache.zeppelin.interpreter.xref.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Skeletal implementation of the Job concept.
 * - designed for inheritance
 * - should be run on a separate thread
 * - maintains internal state: it's status
 * - supports listeners who are updated on status change
 *
 * Job class is serialized/deserialized and used server<->client communication
 * and saving/loading jobs from disk.
 * Changing/adding/deleting non transitive field name need consideration of that.
 */
public abstract class JobImpl<T> implements Job<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobImpl.class);
  private static SimpleDateFormat JOB_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

  private String jobName;
  private String id;

  private Date dateCreated;
  private Date dateStarted;
  private Date dateFinished;
  protected volatile Status status;

  private transient boolean aborted = false;
  private volatile String errorMessage;
  private transient volatile Throwable exception;
  private transient JobListener listener;

  public JobImpl(String jobName, JobListener listener) {
    this.jobName = jobName;
    this.listener = listener;
    dateCreated = new Date();
    id = JOB_DATE_FORMAT.format(dateCreated) + "_" + jobName;
    setStatus(Status.READY);
  }

  public JobImpl(String jobId, String jobName, JobListener listener) {
    this.jobName = jobName;
    this.listener = listener;
    dateCreated = new Date();
    id = jobId;
    setStatus(Status.READY);
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return ((JobImpl) o).id.equals(id);
  }

  @Override
  public Status getStatus() {
    return status;
  }

  /**
   * just set status without notifying to listeners for spell.
   */
  @Override
  public void setStatusWithoutNotification(Status status) {
    this.status = status;
  }

  @Override
  public void setStatus(Status status) {
    if (this.status == status) {
      return;
    }
    Status before = this.status;
    Status after = status;
    this.status = status;
    if (listener != null && before != null && before != after) {
      listener.onStatusChange(this, before, after);
    }
  }

  @Override
  public void setListener(JobListener listener) {
    this.listener = listener;
  }

  @Override
  public JobListener getListener() {
    return listener;
  }

  @Override
  public boolean isTerminated() {
    return !this.status.isReady() && !this.status.isRunning() && !this.status.isPending();
  }

  @Override
  public boolean isRunning() {
    return this.status.isRunning();
  }

  @Override
  public void onJobStarted() {
    dateStarted = new Date();
  }

  @Override
  public void onJobEnded() {
    dateFinished = new Date();
  }

  @Override
  public void run() {
    try {
      onJobStarted();
      completeWithSuccess(jobRun());
    } catch (Throwable e) {
      LOGGER.error("Job failed", e);
      completeWithError(e);
    } finally {
      onJobEnded();
    }
  }

  private void completeWithSuccess(T result) {
    setResult(result);
    exception = null;
    errorMessage = null;
  }

  private void completeWithError(Throwable error) {
    setException(error);
    errorMessage = getJobExceptionStack(error);
  }

  private String getJobExceptionStack(Throwable e) {
    if (e == null) {
      return "";
    }
    Throwable cause = ExceptionUtils.getRootCause(e);
    if (cause != null) {
      return ExceptionUtils.getStackTrace(cause);
    } else {
      return ExceptionUtils.getStackTrace(e);
    }
  }

  @Override
  public Throwable getException() {
    return exception;
  }

  protected void setException(Throwable t) {
    exception = t;
  }

  @Override
  public String getJobName() {
    return jobName;
  }

  @Override
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  protected abstract T jobRun() throws Throwable;

  public abstract boolean jobAbort();

  @Override
  public void abort() {
    aborted = jobAbort();
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public Date getDateCreated() {
    return dateCreated;
  }

  @Override
  public Date getDateStarted() {
    return dateStarted;
  }

  @Override
  public void setDateStarted(Date startedAt) {
    dateStarted = startedAt;
  }

  @Override
  public Date getDateFinished() {
    return dateFinished;
  }

  @Override
  public void setDateFinished(Date finishedAt) {
    dateFinished = finishedAt;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public void setAborted(boolean value) {
    aborted = value;
  }
}
