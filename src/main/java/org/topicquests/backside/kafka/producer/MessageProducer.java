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
package org.topicquests.backside.kafka.producer;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.topicquests.backside.kafka.api.IClosable;
import org.topicquests.support.api.IEnvironment;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author jackpark
 * Generalized so that any MessageProducer can send any topic to any partition/key pair
 * Modeled after a Kafka example
 * @see https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 */
public class MessageProducer extends Thread implements IClosable {
	private IEnvironment environment;
    private final KafkaProducer<String, String> producer;
    private boolean isRunning = true;
   // private boolean isAsync;
    //private final String topic;
    private List<String>messages;
    private List<String>topics;
    private List<Integer>partitions;
    private List<String>keys;
    private List<String>validatedTopics;

	/**
	 * @param e
//	 * @param topic
	 * @param clientId TODO
	 * @param isAsynchronous // ignored for now
	 */
	public MessageProducer(IEnvironment e/*, String clientId, boolean isAsynchronous*/) {
		super();
		environment = e;
//		this.topic = topic;
	//	isAsync = isAsynchronous;
		Properties props = new Properties();
		String url = "localhost";
		String port = "9092";
		if (environment != null && (String)environment.getProperties().get("KAFKA_SERVER_URL") != null) {
			url = (String)environment.getProperties().get("KAFKA_SERVER_URL");
			port = (String)environment.getProperties().get("KAFKA_SERVER_PORT");
		}
		props.put("bootstrap.servers", url+":"+port);
		props.put("acks", "1");
		props.put("retries", "3");
		props.put("linger.ms", "1");
//		props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
//		props.put("client.id", clientId);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		//TODO there may be other necessary key/value pairs
		// but this survived FirstTest
//https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
		producer = new KafkaProducer<String, String>(props);
		messages = new ArrayList<String>();
		topics = new ArrayList<String>();
		validatedTopics = new ArrayList<String>();
		partitions = new ArrayList<Integer>();
		keys = new ArrayList<String>();
		this.start();
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

	void checkValidTopic(String topic) {
		if (!validatedTopics.contains(topic)) {
			//TODO call validateTopic
			validatedTopics.add(topic);
		}
	}
	/**
	 * Send a message
	 * <p><code>key</code> and <code>partition</code> can be the same for messages
	 * in the same <code>topic</code>.</p>
	 * <p>More partitions means more parallelism; a partition is good for about
	 * 10mb/sec, so more parallelism counts</p>
	 * <p>But more partitions add latency to replication</p>
	 * <p>Guidelines: 2000 - 4000 partitions per broker; < 20k partitions
	 * per cluster; max partitions/topic -- 1to2 * #brokers; 10 max</p>
	 * @param topic
	 * @param message
	 * @param key  
	 * @param partition
	 */
	public void sendMessage(String topic, String message, String key, Integer partition) {
		synchronized(messages) {
			checkValidTopic(topic);
			topics.add(topic);
			messages.add(message);
			partitions.add(partition);
			keys.add(key);
			messages.notify();
		}
	}
	public void run() {
		String t = null;
		String msg = null;
		Integer p = null;
		String k = null;
		while (isRunning) {
			synchronized(messages) {
				if (messages.isEmpty()) {
					try {
						messages.wait();
					} catch (Exception e) {}
				} else {
					t = topics.remove(0);
					msg = messages.remove(0);
					p = partitions.remove(0);
					k = keys.remove(0);
				}
			}
			if (msg != null) {
				_sendMessage(t, msg, k, p);
				msg = null;
				p = null;
				k = null;
				t = null;
			}
		}
	}
	
	void _sendMessage(String topic, String msg, String key, Integer partition) {
//https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html		
		ProducerRecord<String, String> pr = 
				new ProducerRecord<String, String>(topic, partition, System.currentTimeMillis(), key, msg);
		//TODO deal with asynch FutureCallback
		System.out.println("ProducerSend "+pr);
		if (environment != null)
			environment.logDebug("ProducerSend "+pr);
		///////////////////
		//Async is recommended since sync is blocking -
		// need a callback
		// see https://github.com/apache/kafka/blob/trunk/examples/src/main/java/kafka/examples/Producer.java
		///////////////////
	//	if (isAsync) {
		//	try {
				producer.send(pr);//.get();
				producer.flush();
				producer.close();
		//	} catch (Exception e) {
		//		if (environment != null)
		//			environment.logError(e.getMessage(), e);
		//		e.printStackTrace();
		//	}			
		//} else {
		//	try {
		//		producer.send(pr); //.get();
		//		producer.flush();
		//		producer.close();
		//	} catch (Exception e) {
		//		if (environment != null)
		//			environment.logError(e.getMessage(), e);
		//		e.printStackTrace();				
		//	}
		//}
	}

	
	/* (non-Javadoc)
	 * @see org.topicquests.backside.kafka.apps.api.IClosable#close()
	 */
	@Override
	public void close() {
		synchronized(messages) {
			isRunning = false;
			producer.close();
		}
	}

}
