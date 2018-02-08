/**
 * 
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
