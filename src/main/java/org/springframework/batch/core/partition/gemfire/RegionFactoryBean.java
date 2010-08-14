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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class RegionFactoryBean<K, V> implements FactoryBean<Region<K, V>>, DisposableBean {

	private static final Log logger = LogFactory.getLog(RegionFactoryBean.class);

	private DistributedSystem distributedSystem;
	private Cache cache;
	private String name = "region";
	private Integer localPort;
	private int[] remotePorts;

	public void destroy() throws Exception {
		if (distributedSystem != null) {
			cache.close();
			distributedSystem.disconnect();
		}
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setLocalPort(int port) {
		this.localPort = port;
	}

	public void setRemotePorts(int[] remotePorts) {
		this.remotePorts = remotePorts;
	}

	public Region<K, V> getObject() throws Exception {

		try {

			cache = CacheFactory.getAnyInstance();
			Region<K, V> region = cache.getRegion("/root/" + name);
			if (region != null) {
				return region;
			}

		} catch (CacheClosedException e) {

			Properties properties = new Properties();
			if (remotePorts != null || localPort!=null) {
				properties.setProperty("mcast-port", "0");
				if (remotePorts!=null) {					
					properties.setProperty("locators", getLocators());
				}
				if (localPort!=null) {
					properties.setProperty("start-locator", String.format("localhost[%d]", localPort));
				}
				logger.debug("DistributedSystem properties: "+properties);
			}
			distributedSystem = DistributedSystem.connect(properties);
			cache = CacheFactory.create(distributedSystem);

		}

		Region<K, V> root = cache.getRegion("/root");
		if (root == null) {
			root = cache.createRegion("root", new AttributesFactory<K, V>().create());
		}
		AttributesFactory<K, V> attributesFactory = new AttributesFactory<K, V>(root.getAttributes());
		attributesFactory.setDataPolicy(DataPolicy.PARTITION);

		return root.createSubregion(name, attributesFactory.create());

	}

	/**
	 * @return
	 */
	private String getLocators() {
		StringBuilder builder = new StringBuilder();
		for (int port : remotePorts) {
			if (builder.length()>0) {
				builder.append(",");
			}
			builder.append(String.format("localhost[%d]", port));
		}
		return builder.toString();
	}

	public Class<?> getObjectType() {
		return Region.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
