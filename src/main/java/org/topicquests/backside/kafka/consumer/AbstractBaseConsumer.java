/*
 * Copyright 2017, TopicQuests
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.topicquests.backside.kafka.api.IClosable;
import org.topicquests.support.api.IEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ShutdownableThread;
import kafka.utils.ZkUtils;

/**
 * @author jackpark
 * @see https://github.com/apache/kafka/tree/trunk/examples
 */
public abstract class AbstractBaseConsumer extends Thread implements IClosable {
	protected IEnvironment environment;
    protected final KafkaConsumer<String, String> consumer;
    private final String topic;
    protected boolean isRunning = true;
    private ConsumerRecords<String, String> records = null;

    
	/**
	 * A consumer is interested in its <code>groupId</code> and its
	 * <code>topic</code>. It could be generalized to take a list of topics,
	 * but this code supports one consumer for a topic.
	 * @param e
	 * @param groupId can be <code>null</code>
	 * @param topic
	 */
	public AbstractBaseConsumer(IEnvironment e, String groupId, String topic) {
// 		super(topic, false);
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
		props.put("bootstrap.servers", url+":"+port);
		props.put("group.id", gid); //TODO not sure about that
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("enable.auto.commit", "false"); //if false, you have to commit later
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
//	    props.put("session.timeout.ms", "30000");
 	    //TODO there may be other key/values but these survived FirstText
//https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
		consumer = new KafkaConsumer<String, String>(props);
		//Kafka Consumer subscribes list of topics here.
		List<String>x = Collections.singletonList(topic);
		environment.logDebug("AbstractBaseConsumer- "+x);
		consumer.subscribe(x);
		//TODO call validateTopic
//		this.start();
		
	}

	/**
	 * Validate the <code>topic</code>. If missing, create it.
	 * @param util
	 * @param topic
	 * @param partitions
	 * @param rep
	 * @param properties
	 * @param m
	 */
	void validateTopic(ZkUtils util, String topic,
			Integer partitions, Integer rep, Properties properties, RackAwareMode m) {
		if(!AdminUtils.topicExists(util, topic)){
		    AdminUtils.createTopic(util, topic, partitions, rep, properties, m);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.topicquests.backside.kafka.apps.api.IClosable#close()
	 */
	@Override
	public void close() {
		isRunning = false;
	}
	
	/**
	 * Extension class must implement this to do whatever it wants
	 * with the <code>records</code>
	 * @param record
	 * @return TODO
	 */
	public abstract boolean handleRecord(ConsumerRecord<String, String> record);
	
	/**
	 * Extension class must call this once it has the listener engaged
	 */
	public void begin() {
		this.start();
	}

	public void run() {
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
	 				isHandled = handleRecord(cr);
	 			//	if (isHandled)
	 			//		consumer.commitSync();
	 			}
	 			consumer.commitSync();
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
