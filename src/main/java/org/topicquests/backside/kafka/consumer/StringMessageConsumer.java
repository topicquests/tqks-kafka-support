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
package org.topicquests.backside.kafka.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.support.api.IEnvironment;

/**
 * @author jackpark
 * Modeled after a Kafka example
 * @see https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 * <p>
 * This is a very simple message consumer:
 * <ul>
 * <li>It only looks in Partition 0</li>
 * <li>If told, it will rewind offset in that partition to 0 on bootup</li>
 * </ul>
 * <p>
 * 
 */
public class StringMessageConsumer extends Thread {
    private IMessageConsumerListener listener;
	protected IEnvironment environment;
    protected final KafkaConsumer<String, String> consumer;
    private final String topic;
    protected boolean isRunning = true;
    private ConsumerRecords<String, String> records = null;

	/**
	 * @param e
	 * @param groupId
	 * @param topic
	 * @param l
	 * @param isRewind
	 */
	public StringMessageConsumer(IEnvironment e, String groupId, String topic, IMessageConsumerListener l, boolean isRewind) {
		listener = l;
 		this.topic = topic;
        System.out.println("ABC "+topic);
		environment = e;
		String gid = groupId;
		if (groupId == null)
			gid = topic;
		Properties props = new Properties();
		String url = "localhost";
		String port = "9092";
		if (environment != null && (String)environment.getProperties().get("KAFKA_SERVER_URL") != null) {
			url = (String)environment.getProperties().get("KAFKA_SERVER_URL");
			port = (String)environment.getProperties().get("KAFKA_SERVER_PORT");
		}
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url+":"+port);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, gid); //TODO not sure about that
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //if false, you have to commit later
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//	    props.put("session.timeout.ms", "30000");
		consumer = new KafkaConsumer<String, String>(props);
		TopicPartition tp = new TopicPartition(topic, 0);
		List<TopicPartition> partitions = Collections.singletonList(tp);
		consumer.assign(partitions);
		if (isRewind) //Rewind to the beginning of the partition
			consumer.seek(tp, 0);
		this.start();
	}

	public void close() {
		isRunning = false;
	}

	public void run() {
		System.out.println("Consumer started");
		while (isRunning) {
	          records = consumer.poll(1000);
	          if (!isRunning)
	        	  return;
	         if (records != null && records.count() > 0) {
	 			System.out.println("AbstractBaseConsumer "+records);
	 			if (environment != null)
	 				environment.logDebug("ConsumerGet "+records);
	 			boolean isHandled = false;
	 			for (ConsumerRecord cr: records) {
	 				isHandled = listener.acceptRecord(cr);
	 			}
//	 			consumer.commitSync(); we are using autoCommit
	 			///////////////
	 			//Interesting edge case:
	 			// suppose some record is not handled
	 			// then we don't reset the pointer
	 			// which might mean it will show up immediately when this exits
	 			// in the next pass -- could end up in an endless loop on a 
	 			// failing record.
	 			///////////////
	         }
		}
	}

}
