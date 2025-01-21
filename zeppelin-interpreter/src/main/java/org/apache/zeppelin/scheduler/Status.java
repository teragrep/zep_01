package org.apache.zeppelin.scheduler;

/**
 * Job status.
 * UNKNOWN - Job is not found in remote READY - Job is not running, ready to run. PENDING - Job is submitted to
 * scheduler. but not running yet RUNNING - Job is running. FINISHED - Job finished run. with success ERROR - Job
 * finished run. with error ABORT - Job finished by abort
 */
public enum Status {
    UNKNOWN, READY, PENDING, RUNNING, FINISHED, ERROR, ABORT;

    public boolean isReady() {
        return this == READY;
    }

    public boolean isRunning() {
        return this == RUNNING;
    }

    public boolean isPending() {
        return this == PENDING;
    }

    public boolean isCompleted() {
        return this == FINISHED || this == ERROR || this == ABORT;
    }
}
