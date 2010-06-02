/*
 * Copyright 2006-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.batch.core.partition.gemfire;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;

/**
 * Main entry and configuration point for users of Gemfire as a partitioned step execution grid. Configure one of these
 * as the partition handler in a partitioned step and make sure that the Gemfire jars are on the classpath (and in the
 * distribution lib directory).
 * 
 * @author Dave Syer
 * 
 */
public class GemfirePartitionHandler implements PartitionHandler, InitializingBean {

	private int gridSize = 1;

	private Region<String, StepExecution> region;

	private Step step;

	/**
	 * Storage area and transport for step execution requests in the distributed system.
	 * 
	 * @param region a partitioned Region
	 */
	public void setRegion(Region<String, StepExecution> region) {
		this.region = region;
	}

	/**
	 * @param step the step to set
	 */
	public void setStep(Step step) {
		this.step = step;
	}

	/**
	 * The number of step executions to create per step execution. Can but does not have to be the same as the physical
	 * grid size (number of nodes). Setting to a multiple of the physical grid size has some advantages regarding load
	 * balancing if the partitioned step executions are not of uniform duration.
	 * 
	 * @param gridSize the grid size to set
	 */
	public void setGridSize(int gridSize) {
		this.gridSize = gridSize;
	}

	/**
	 * Check mandatory properties (region, step name, config location).
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.state(region != null, "A Region must be provided");
		Assert.state(step != null, "A step must be provided");
	}

	/**
	 * Handles the task generation, distribution and result collation for a partitioned step execution in a Gemfire
	 * distributed system. For each partitioned execution puts an entry in the cache region provided, and removes all
	 * entries after the step has finished.
	 * 
	 * @see PartitionHandler#handle(StepExecutionSplitter, StepExecution)
	 */
	public Collection<StepExecution> handle(StepExecutionSplitter stepSplitter, StepExecution masterStepExecution)
			throws Exception {

		Set<StepExecution> split = stepSplitter.split(masterStepExecution, gridSize);
		String prefix = UUID.randomUUID().toString();
		Set<String> keys = new HashSet<String>();

		for (StepExecution stepExecution : split) {
			String key = prefix + ":" + stepExecution.getStepName();
			region.put(key, stepExecution);
			keys.add(key);
		}

		try {

			Execution execution = FunctionService.onRegion(region).withFilter(keys);
			ResultCollector<? extends Serializable> collector = execution.execute(new GemfirePartitionFunction(step));

			@SuppressWarnings("unchecked")
			Collection<StepExecution> result = (Collection<StepExecution>) collector.getResult();
			return result;

		} finally {

			for (String key : keys) {
				region.remove(key);
			}

		}

	}

}
