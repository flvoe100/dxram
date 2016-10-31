
package de.hhu.bsinfo.dxcompute.job.ws;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;

/**
 * Delegate for worker to provide some information to the Component via callbacks.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface WorkerDelegate {
    /**
     * Steal a job from the local queue.
     * @param p_thief
     *            Worker trying to steal.
     * @return Job successfully stolen from another worker or null if no jobs available or stealing failed.
     */
    AbstractJob stealJobLocal(final Worker p_thief);

    /**
     * A job was scheduled for execution by a worker.
     * @param p_job
     *            Job which was scheduled.
     */
    void scheduledJob(final AbstractJob p_job);

    /**
     * A job is right before getting executed by a worker.
     * @param p_job
     *            Job getting executed.
     */
    void executingJob(final AbstractJob p_job);

    /**
     * Execution of a job finished by a worker.
     * @param p_job
     *            Job that finished execution.
     */
    void finishedJob(final AbstractJob p_job);

    /**
     * Get the ID of the Node the worker is running on.
     * @return NodeID.
     */
    short getNodeID();
}
