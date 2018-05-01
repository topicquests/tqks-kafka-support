/*
 * Copyright 2017,2018 TopicQuests
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
import org.apache.kafka.clients.producer.ProducerConfig;
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
 * <p>
 * This is a very simple MessageProducer (String):
 * It only placed its given topic in Partition 0
 * </p>
 */
public class MessageProducer extends Thread implements IClosable {
	protected IEnvironment environment;
    private final KafkaProducer<String, String> producer;
    private boolean isRunning = true;
    private List<String>messages;
    private List<String>topics;
    private List<Integer>partitions;
    private List<String>keys;
    private List<String>validatedTopics;

	/**
	 * @param e
	 * @param clientId TODO
	 */
	public MessageProducer(IEnvironment e, String clientId) {
		super();
		environment = e;
		Properties props = new Properties();
		String url = "localhost";
		String port = "9092";
		if (environment != null && (String)environment.getProperties().get("KAFKA_SERVER_URL") != null) {
			url = (String)environment.getProperties().get("KAFKA_SERVER_URL");
			port = (String)environment.getProperties().get("KAFKA_SERVER_PORT");
		}
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url+":"+port);
		props.put("acks", "all");
		props.put("retries", "0");
		props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		environment.logDebug("MessageProducer-");
		producer = new KafkaProducer<String, String>(props);
		messages = new ArrayList<String>();
		topics = new ArrayList<String>();
		validatedTopics = new ArrayList<String>();
		partitions = new ArrayList<Integer>();
		keys = new ArrayList<String>();
		this.start();
		environment.logDebug("MessageProducer+");
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
		environment.logDebug("MessageProducer.checkValidTopic "+topic+" "+validatedTopics);
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
		environment.logDebug("MessageProducer.sendMessage "+messages.size());
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
		ProducerRecord<String, String> pr = 
			new ProducerRecord<String, String>(topic, partition, System.currentTimeMillis(), key, msg);
		System.out.println("ProducerSend "+pr);
		producer.send(pr); 
		if (environment != null)
			environment.logDebug("ProducerSend "+pr);
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
