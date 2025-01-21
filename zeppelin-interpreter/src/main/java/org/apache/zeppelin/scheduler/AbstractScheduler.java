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

import org.apache.zeppelin.interpreter.xref.Code;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.xref.Job;
import org.apache.zeppelin.interpreter.xref.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract class for scheduler implementation. Implementor just need to implement method
 * runJobInScheduler.
 */
public abstract class AbstractScheduler implements Scheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScheduler.class);

  protected String name;
  protected volatile boolean terminate = false;
  protected BlockingQueue<Job> queue = new LinkedBlockingQueue<>();
  protected Map<String, Job> jobs = new ConcurrentHashMap<>();
  private Thread schedulerThread;

  public AbstractScheduler(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public List<Job> getAllJobs() {
    return new ArrayList<>(jobs.values());
  }

  @Override
  public Job getJob(String jobId) {
    return jobs.get(jobId);
  }

  @Override
  public void submit(Job job) {
    job.setStatus(Status.PENDING);
    try {
      queue.put(job);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format("Unable to submit job %s", job.getId()), e);
    }
    jobs.put(job.getId(), job);
  }

  @Override
  public Job cancel(String jobId) {
    Job job = jobs.remove(jobId);
    job.abort();
    return job;
  }

  @Override
  public void run() {
    schedulerThread = Thread.currentThread();
    while (!terminate && !schedulerThread.isInterrupted()) {
      Job runningJob = null;
      try {
        runningJob = queue.take();
      } catch (InterruptedException e) {
        LOGGER.warn("{} is interrupted", getClass().getSimpleName());
        break;
      }

      runJobInScheduler(runningJob);
    }
  }

  public abstract void runJobInScheduler(Job job);

  @Override
  public void stop() {
    terminate = true;
    for (Job job : queue) {
      job.setAborted(true);
      job.jobAbort();
    }
    if (schedulerThread != null) {
      schedulerThread.interrupt();
    }
  }

  /**
   * This is the logic of running job.
   * Subclass can use this method and can customize where and when to run this method.
   *
   * @param runningJob
   */
  protected void runJob(Job runningJob) {
    if (runningJob.isAborted()) {
      LOGGER.info("Job {} is aborted", runningJob.getId());
      runningJob.setStatus(Status.ABORT);
      runningJob.setAborted(false);
      return;
    }

    LOGGER.info("Job {} started by scheduler {}",runningJob.getId(), name);
    // Don't set RUNNING status when it is RemoteScheduler, update it via JobStatusPoller
    if (!getClass().getSimpleName().equals("RemoteScheduler")) {
      runningJob.setStatus(Status.RUNNING);
    }
    runningJob.run();
    Object jobResult = runningJob.getReturn();
    synchronized (runningJob) {
      if (runningJob.isAborted()) {
        runningJob.setStatus(Status.ABORT);
        LOGGER.debug("Job Aborted, " + runningJob.getId() + ", " +
                runningJob.getErrorMessage());
      } else if (runningJob.getException() != null) {
        LOGGER.debug("Job Error, " + runningJob.getId() + ", " +
                runningJob.getReturn());
        runningJob.setStatus(Status.ERROR);
      } else if (jobResult != null && jobResult instanceof InterpreterResult
              && ((InterpreterResult) jobResult).code() == Code.ERROR) {
        LOGGER.debug("Job Error, " + runningJob.getId() + ", " +
                runningJob.getReturn());
        runningJob.setStatus(Status.ERROR);
      } else {
        LOGGER.debug("Job Finished, " + runningJob.getId() + ", Result: " +
                runningJob.getReturn());
        runningJob.setStatus(Status.FINISHED);
      }
    }
    LOGGER.info("Job {} finished by scheduler {} with status {}", runningJob.getId(), name, runningJob.getStatus());
    // reset aborted flag to allow retry
    runningJob.setAborted(false);
    jobs.remove(runningJob.getId());
  }
}
