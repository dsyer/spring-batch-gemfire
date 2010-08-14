package org.springframework.batch.core.partition.gemfire;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public abstract class RegionLauncher {

	private static final Log logger = LogFactory.getLog(RegionLauncher.class);

	public static void main(String[] args) throws Exception {
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/launch-context.xml");
		@SuppressWarnings("unchecked")
		Region<String, StepExecution> region = context.getBean("region", Region.class);
		assertNotNull(region);
		region.getAttributesMutator().addCacheListener(new SimpleCacheListener<String,StepExecution>());

		System.out.println("DistributedSystem started.  Press return to end, or a non-blank line to list the region.");
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String line = reader.readLine();
		while (line.length() > 0) {
			for (Entry<String, StepExecution> entry : region.entrySet()) {
				logger.debug("Entry: " + entry.getKey() + ": "+entry.getValue());
			}
			line = reader.readLine();
		}

		context.close();

	}

	public static class SimpleCacheListener<K, V> extends CacheListenerAdapter<K, V> implements Declarable {

		private static Log logger = LogFactory.getLog(SimpleCacheListener.class);
		
		public void afterCreate(EntryEvent<K, V> e) {
			logger.debug("Received afterCreate event for entry: " + e.getKey() + ", " + e.getNewValue());
		}

		public void afterUpdate(EntryEvent<K, V> e) {
			logger.debug("Received afterUpdate event for entry: " + e.getKey() + ", " + e.getNewValue());
		}

		public void afterDestroy(EntryEvent<K, V> e) {
			logger.debug("Received afterDestroy event for entry: " + e.getKey());
		}

		public void afterInvalidate(EntryEvent<K, V> e) {
			logger.debug("Received afterInvalidate event for entry: " + e.getKey());
		}

		public void afterRegionLive(RegionEvent<K, V> e) {
			logger.debug("Received afterRegionLive event, sent to durable clients after \nthe server has finished replaying stored events.  ");
		}

		public void init(Properties props) {
			// do nothing
		}

	}

}
