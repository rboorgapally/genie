/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.server.metrics;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Utility class to get number of jobs running on this instance.
 *
 * @author skrishnan
 * @author tgianos
 */
public interface JobCountManager {

    /**
     * Get number of running jobs running on this instance.
     *
     * @return number of running jobs on this instance
     * @throws CloudServiceException if there is an error
     */
    int getNumInstanceJobs() throws CloudServiceException;

    /**
     * Get number of running jobs with minStartTime &lt; startTime &gt;
     * maxStartTime on this instance min/max startTimes are ignored if they are
     * null.
     *
     * @param minStartTime min start time in ms
     * @param maxStartTime max start time in ms
     * @return number of running jobs between the specified times
     * @throws CloudServiceException if there is an error
     */
    int getNumInstanceJobs(
            final Long minStartTime,
            final Long maxStartTime)
            throws CloudServiceException;

    /**
     * Get number of running jobs with minStartTime <= startTime < maxStartTime
     * min/max startTimes are ignored if they are null.
     *
     * @param hostName - if null, local host name is used
     * @param minStartTime min start time in ms
     * @param maxStartTime max start time in ms
     * @return number of running jobs matching specified critiera
     * @throws CloudServiceException if there is an error
     */
    int getNumInstanceJobs(
            String hostName,
            final Long minStartTime,
            final Long maxStartTime) throws CloudServiceException;

    /**
     * Returns the most idle Genie instance (&lt; minJobThreshold running jobs),
     * if possible - else returns current instance.
     *
     * @param minJobThreshold the threshold to use to look for idle instances
     * @return host name of most idle Genie instance
     * @throws CloudServiceException if there is any error
     */
    String getIdleInstance(final long minJobThreshold) throws CloudServiceException;
}
