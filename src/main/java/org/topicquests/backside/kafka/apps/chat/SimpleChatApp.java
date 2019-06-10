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
package org.topicquests.backside.kafka.apps.chat;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.topicquests.backside.kafka.apps.AbstractKafkaApp;
import org.topicquests.backside.kafka.consumer.AbstractBaseConsumer;
import org.topicquests.backside.kafka.consumer.StringMessageConsumer;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.backside.kafka.producer.MessageProducer;
import org.topicquests.support.RootEnvironment;

/**
 * @author jackpark
 * Just a really trivial app to sort out how to make Consumers and Producers
 * Improvements to this would be to put structure in the message, e.g.
 * {id:"joe", msg:"<message>"}
 */
public class SimpleChatApp extends AbstractKafkaApp 
		implements IMessageConsumerListener {
	private MessageProducer producer;
	private StringMessageConsumer consumer;
	private String clientId;
	private final String outTopic;
	private ChatUI ui;
	
	/**
	 * @param env
	 */
	public SimpleChatApp(ChatUI ui) {
		this.ui = ui;
		clientId = Long.toString(System.currentTimeMillis()); //TODO make into a config value
		outTopic = getStringProperty("ChatProducerTopic");
		// sends on a topic
		producer = new MessageProducer(this, outTopic);
		// consumer listens to the sam topic
		consumer = new StringMessageConsumer(this, clientId, outTopic, this, true);
	}

	public void sendMessage(String msg) {
		logDebug("ChatAppSendMessage "+msg);
		producer.sendMessage(outTopic, msg, outTopic, new Integer(0));
	}
	

	@Override
	public void close() {
		producer.close();
		consumer.close();
	}

	@Override
	public boolean acceptRecord(ConsumerRecord record) {
		String text = (String)record.value();
		ui.say(text);
		return true;
	}

	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
	}

}
