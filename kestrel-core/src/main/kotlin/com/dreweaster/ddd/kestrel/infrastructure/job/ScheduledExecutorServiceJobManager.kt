package com.dreweaster.ddd.kestrel.infrastructure.job

import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.infrastructure.cluster.ClusterManager
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.time.toKotlinDuration

class ScheduledExecutorServiceJobManager(private val clusterManager: ClusterManager, private val scheduler: ScheduledExecutorService) :
    JobManager {
    private val logger = LoggerFactory.getLogger(ScheduledExecutorServiceJobManager::class.java)

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job) {
        scheduleManyTimes(
            repeatSchedule = repeatSchedule,
            timeout = repeatSchedule.multipliedBy(10),
            job = job,
            eagerRetry = true,
        )
    }

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job, timeout: Duration, eagerRetry: Boolean) {
        logger.debug("Scheduling job: '${job.name}'")
        // It's okay to block waiting for a future result as we're using a dedicated job execution context
        // It's important that we wait for a job to complete execution
        // so that it's not rescheduled if the previous invocation hasn't yet completed
        scheduler.scheduleAtFixedRate(
            {
                try {
                    runBlocking {
                        runJob(repeatSchedule, job, timeout, eagerRetry)
                    }
                } catch (ex: Exception) {
                    logger.error("Job execution failed: '${job.name}'", ex)
                }
            },
            repeatSchedule.toMillis(),
            repeatSchedule.toMillis(),
            TimeUnit.MILLISECONDS,
        )
    }

    private suspend fun runJob(repeatSchedule: Duration, job: Job, timeoutMs: Duration, eagerRetry: Boolean) {
        val backlogSize = withTimeout(timeoutMs.toKotlinDuration()) {
            ClusterSingletonJobWrapper(job).execute()
        }
        // Repeat immediately if there is a backlog; do not wait for repeat schedule to process the backlog
        if (eagerRetry && backlogSize > 0) {
            runJob(repeatSchedule, job, timeoutMs, true)
        }
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override suspend fun execute(): Long = if (clusterManager.iAmTheLeader()) {
            logger.debug("Running job '$name' as this instance is leader")
            wrappedJob.execute()
        } else {
            logger.debug("Not running job '$name' as this instance is not leader")
            0L
        }
    }
}
