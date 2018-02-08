/**
 * 
 */
package org.topicquests.backside.kafka.producer.api;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author jackpark
 *
 */
public interface IProducerListener {

	/**
	 * Accept results from a Producer
	 * @param metadata
	 * @param exception
	 */
	void accept(RecordMetadata metadata, Exception exception);
}
