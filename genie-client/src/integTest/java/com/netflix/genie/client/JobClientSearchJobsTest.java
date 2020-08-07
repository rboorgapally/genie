/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.client;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.List;

/**
 * Integration tests for {@link JobClient} getJobs search.
 *
 * @since 3.0.0
 */
abstract class JobClientSearchJobsTest extends JobClientIntegrationTest {

    @Test
    void canSearchJobs() throws Exception {
        this.createJobs();

        this.jobClient.waitForCompletion(sleepJobId, 60000, 100);
        this.jobClient.waitForCompletion(killJobId, 60000, 100);
        this.jobClient.waitForCompletion(timeoutJobId, 60000, 100);
        this.jobClient.waitForCompletion(dateJobId, 60000, 100);
        this.jobClient.waitForCompletion(echoJobId, 60000, 100);

        Assertions
            .assertThat(this.jobClient.getJobs())
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(sleepJobId, killJobId, timeoutJobId, dateJobId, echoJobId);
        Assertions
            .assertThat(
                this.jobClient.getJobs(
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    echoCommandId,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(echoJobId);
        Assertions
            .assertThat(
                this.jobClient.getJobs(
                    null,
                    null,
                    null,
                    Sets.newHashSet(JobStatus.KILLED.name()),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(killJobId, timeoutJobId);

        for (int pageNumber = 0; pageNumber < 5; pageNumber++) {
            final List<JobSearchResult> result = this.jobClient.getJobs(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                pageNumber,
                1,
                null,
                null
            );
            Assertions.assertThat(result.size()).isEqualTo(1);
        }
    }
}
