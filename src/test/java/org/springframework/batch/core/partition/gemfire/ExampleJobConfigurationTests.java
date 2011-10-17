/*
 * Copyright 2006-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.partition.gemfire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Simple test case demonstrating the gemfire partitioner. Should run out of the
 * box on its own with an in memory database for the job repository. Change the
 * context location to <code>launch-context.xml</code> to run in a gem cluster.
 * 
 * @author Dave Syer
 * 
 */
@ContextConfiguration(locations = { "/test-context.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
public class ExampleJobConfigurationTests {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;

	@Autowired
	@Qualifier("step")
	private Step step;

	@Test
	public void testSimpleProperties() throws Exception {
		assertNotNull(jobLauncher);
		assertNotNull(step);
	}

	@Test
	public void testLaunchJob() throws Exception {
		JobExecution result = jobLauncher.run(job, new JobParametersBuilder().addString("run.id", "integration.test")
				.toJobParameters());
		assertNotNull(result);
		assertEquals(BatchStatus.COMPLETED, result.getStatus());
		assertEquals(21, result.getStepExecutions().size());
	}

}
