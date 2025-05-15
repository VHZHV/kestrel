package com.dreweaster.ddd.kestrel.application.job

import java.time.Duration

interface Job {
    val name: String

    suspend fun execute(): Long
}

interface JobManager {
    fun scheduleManyTimes(repeatSchedule: Duration, job: Job)
    fun scheduleManyTimes(repeatSchedule: Duration, job: Job, timeout: Duration, eagerRetry: Boolean)
}
