/*
 * Copyright 2019, TopicQuests
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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.support.api.IEnvironment;

/**
 * @author jackpark
 *
 */
public class StringConsumer extends Thread {
	private IEnvironment environment;
    private IMessageConsumerListener listener;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private boolean isRunning = true;
    private ConsumerRecords<String, String> records = null;
    private final boolean rewind;
    private final int pollTime;

	/**
	 * 
	 * @param env
	 * @param groupId
	 * @param topic
	 * @param l
	 * @param isRewind -- can boot to rewind
	 * @param pollingSeconds
	 */
	public StringConsumer(IEnvironment env, String groupId, String topic,
			IMessageConsumerListener l, boolean isRewind, int pollingSeconds) {
		environment = env;
		listener = l;
		this.topic = topic;
		this.rewind = isRewind;
		this.pollTime = pollingSeconds;
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
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 25000);
		//props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000);
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList(this.topic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// TODO Auto-generated method stub
				
			}
			//@see https://stackoverflow.com/questions/54480715/no-current-assignment-for-partition-occurs-even-after-poll-in-kafka
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				if (rewind) {
	               for (TopicPartition tp : partitions) {
	            	   consumer.seek(tp, 0);
	                }
				}
				
			}
        });
		isRunning = true;
		this.start();
	}

	public void close() {
		isRunning = false;
	}

	public void run() {
		System.out.println("Consumer started");
//		if (rewind) {//Rewind to the beginning of the partition
//			TopicPartition tp = new TopicPartition(topic, 0);
//			consumer.seek(tp, 0);
//		}
		boolean isHandled = false;
		while (isRunning) {
			System.out.println("Consumer c1");
	          records = consumer.poll(Duration.ofSeconds(pollTime));
	          System.out.println("Consumer c2 "+records.count());
	          if (!isRunning)
	        	  return;
	         if (records != null && records.count() > 0) {
	 			System.out.println("AbstractBaseConsumer "+records);
	 			if (environment != null)
	 				environment.logDebug("ConsumerGet "+records);
	 			isHandled = false;
	 			for (ConsumerRecord cr: records) {
	 				isHandled = listener.acceptRecord(cr);
	 				//if (isHandled)
	 				//	consumer.commitSync();
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
