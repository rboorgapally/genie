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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Integration tests for {@link JobClient}.
 *
 * @author amsharma
 * @since 3.0.0
 */
abstract class JobClientIntegrationTest extends ClusterClientIntegrationTest {

    private static final String DATE_TAG = "type:date";
    private static final String ECHO_TAG = "type:echo";
    private static final String SLEEP_TAG = "type:sleep";
    private static final String DUMMY_TAG = "type:dummy";

    protected String dummyClusterId;
    protected String sleepCommandId;
    protected String dateCommandId;
    protected String echoCommandId;

    protected JobRequest sleepJob;
    protected JobRequest killJob;
    protected JobRequest timeoutJob;
    protected JobRequest dateJob;
    protected JobRequest echoJob;

    protected String sleepJobId;
    protected String killJobId;
    protected String timeoutJobId;
    protected String dateJobId;
    protected String echoJobId;

    @Test
    void canSubmitJob() throws Exception {
        this.createJobs();

        Assertions
            .assertThat(this.jobClient.waitForCompletion(sleepJobId, 60000, 100))
            .isEqualByComparingTo(JobStatus.SUCCEEDED);

        Assertions
            .assertThat(this.jobClient.waitForCompletion(killJobId, 60000, 100))
            .isEqualByComparingTo(JobStatus.KILLED);

        Assertions
            .assertThat(this.jobClient.waitForCompletion(timeoutJobId, 60000, 100))
            .isEqualByComparingTo(JobStatus.KILLED);

        Assertions
            .assertThat(this.jobClient.waitForCompletion(dateJobId, 60000, 100))
            .isEqualByComparingTo(JobStatus.SUCCEEDED);

        Assertions
            .assertThat(this.jobClient.waitForCompletion(echoJobId, 60000, 100))
            .isEqualByComparingTo(JobStatus.SUCCEEDED);

        // Some basic checking of fields
        Assertions.assertThat(this.jobClient.getJob(sleepJobId).getName()).isEqualTo(sleepJob.getName());
        Assertions.assertThat(this.jobClient.getJobRequest(sleepJobId).getUser()).isEqualTo(sleepJob.getUser());
        Assertions.assertThat(this.jobClient.getJobExecution(sleepJobId)).isNotNull();
        Assertions.assertThat(this.jobClient.getJobMetadata(sleepJobId)).isNotNull();
        Assertions.assertThat(this.jobClient.getJobCluster(sleepJobId).getId()).isPresent().contains(dummyClusterId);
        Assertions.assertThat(this.jobClient.getJobCommand(sleepJobId).getId()).isPresent().contains(sleepCommandId);
        Assertions.assertThat(this.jobClient.getJobCommand(dateJobId).getId()).isPresent().contains(dateCommandId);
        Assertions.assertThat(this.jobClient.getJobCommand(echoJobId).getId()).isPresent().contains(echoCommandId);
        Assertions.assertThat(this.jobClient.getJobApplications(sleepJobId)).isEmpty();
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId), StandardCharsets.UTF_8))
            .isEqualTo("hello\n");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStderr(echoJobId), StandardCharsets.UTF_8))
            .isBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, "stdout"), StandardCharsets.UTF_8))
            .isEqualTo("hello\n");
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, "run"), StandardCharsets.UTF_8))
            .isNotBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, ""), StandardCharsets.UTF_8))
            .isNotBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, null), StandardCharsets.UTF_8))
            .isNotBlank();

        // Some quick find jobs calls
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
    }

    protected void createJobs() throws Exception {
        dummyClusterId = this.createDummyCluster();
        sleepCommandId = this.createSleepCommand();
        dateCommandId = this.createDateCommand();
        echoCommandId = this.createEchoCommand();

        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(DUMMY_TAG))
        );

        sleepJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("1"))
            .withDisableLogArchival(true)
            .build();

        killJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("60"))
            .withDisableLogArchival(true)
            .build();

        timeoutJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("60"))
            .withTimeout(1)
            .withDisableLogArchival(true)
            .build();

        dateJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(DATE_TAG)
        )
            .withDisableLogArchival(true)
            .build();

        echoJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(ECHO_TAG)
        )
            .withCommandArgs(Lists.newArrayList("hello"))
            .withDisableLogArchival(true)
            .build();

        final byte[] attachmentBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(attachmentBytes)) {
            final Map<String, InputStream> attachments = ImmutableMap.of("attachment.txt", bis);
            sleepJobId = this.jobClient.submitJobWithAttachments(sleepJob, attachments);
        }
        killJobId = this.jobClient.submitJob(killJob);
        final Thread killThread = new Thread(
            () -> {
                try {
                    while (this.jobClient.getJobStatus(killJobId) != JobStatus.RUNNING) {
                        Thread.sleep(10);
                    }
                    this.jobClient.killJob(killJobId);
                } catch (final Exception e) {
                    Assertions.fail(e.getMessage(), e);
                }
            }
        );
        killThread.start();
        timeoutJobId = this.jobClient.submitJob(timeoutJob);
        dateJobId = this.jobClient.submitJob(dateJob);
        echoJobId = this.jobClient.submitJob(echoJob);
    }

    protected String createDummyCluster() throws Exception {
        return this.clusterClient.createCluster(
            new Cluster.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            )
                .withTags(Sets.newHashSet(DUMMY_TAG, UUID.randomUUID().toString()))
                .build()
        );
    }

    protected String createSleepCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("sleep"),
                100
            )
                .withTags(Sets.newHashSet(SLEEP_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }

    protected String createDateCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("date"),
                100
            )
                .withTags(Sets.newHashSet(DATE_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }

    protected String createEchoCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("echo"),
                100
            )
                .withTags(Sets.newHashSet(ECHO_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }
}
