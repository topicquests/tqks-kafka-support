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
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.topicquests.backside.kafka.api.IClosable;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.support.api.IEnvironment;

//import kafka.utils.ShutdownableThread;

/**
 * @author jackpark
 * Modeled after a Kafka example
 * @see https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 */
public class StringMessageConsumer extends AbstractBaseConsumer {
    private IMessageConsumerListener listener;
    private boolean isRunning = true;

	/**
	 * @param e
	 * @param groupId
	 * @param topic
	 * @param l
	 */
	public StringMessageConsumer(IEnvironment e, String groupId, String topic, IMessageConsumerListener l) {
		super(e, groupId, topic);
		listener = l;
	}

	/* (non-Javadoc)
	 * @see org.topicquests.backside.kafka.apps.api.IClosable#close()
	 */
	@Override
	public void close() {
		isRunning = false;
		consumer.close();
	}

	@Override
	public boolean handleRecord(ConsumerRecord<String, String> record) {
//        if (record != null) {
       	 environment.logDebug("ConsumerGet "+record);
       	 //TODO should send just one record and
       	 return listener.acceptRecord(record);
 //       }
	}

}
