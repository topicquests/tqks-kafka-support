/*
 * Copyright 2017, 2018 TopicQuests
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.topicquests.backside.kafka.connector;

import org.topicquests.support.api.IEnvironment;

/**
 * @author jackpark
 *
 */
public abstract class AbstractBaseConnector {
	protected IEnvironment environment;

	/**
	 * 
	 */
	public AbstractBaseConnector(IEnvironment env) {
		environment = env;
	}

	public abstract void close();
	
	/**
	 * Allow extension to do its own initializations
	 */
	public abstract void initialize();
}
