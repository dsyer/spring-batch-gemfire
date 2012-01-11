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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.UnexpectedJobExecutionException;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

/**
 * A serializable function that can execute a step as part of a partition
 * execution. Used internally by the {@link GemfirePartitionHandler}.
 * 
 * @author Dave Syer
 * 
 */
public class GemfirePartitionFunction extends FunctionAdapter {

	private static Log logger = LogFactory.getLog(GemfirePartitionFunction.class);

	private final Step step;

	public GemfirePartitionFunction(Step step) {
		this.step = step;
	}

	@Override
	public void execute(FunctionContext context) {
		execute(context.<StepExecution> getResultSender(), extract((RegionFunctionContext) context));
	}

	private void execute(ResultSender<StepExecution> sender, List<StepExecution> executors) {
		int count = 1;
		for (StepExecution executor : executors) {
			count++;
			if (count < executors.size()) {
				sender.sendResult(execute(executor));
			}
			else {
				sender.lastResult(execute(executor));
			}
		}
	}

	private StepExecution execute(StepExecution stepExecution) {
		try {
			step.execute(stepExecution);
		}
		catch (JobInterruptedException e) {
			stepExecution.getJobExecution().setStatus(BatchStatus.STOPPING);
			throw new UnexpectedJobExecutionException("TODO: this should result in a stop", e);
		}
		return stepExecution;
	}

	private List<StepExecution> extract(RegionFunctionContext context) {

		Map<String, StepExecution> data = PartitionRegionHelper.getLocalDataForContext(context);
		@SuppressWarnings("unchecked")
		Set<String> keys = (Set<String>) context.getFilter();

		List<StepExecution> result = new ArrayList<StepExecution>();

		for (String key : keys) {
			logger.debug("Extract:" + key);
			StepExecution execution = data.get(key);
			if (execution != null) {
				result.add(execution);
			}
		}

		return result;

	}

	@Override
	public String getId() {
		return getClass().getSimpleName() + ":" + step.getName();
	}

}
