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
package devtests;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.topicquests.backside.kafka.consumer.StringMessageConsumer;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.backside.kafka.producer.MessageProducer;
import org.topicquests.support.RootEnvironment;
import org.topicquests.support.api.IEnvironment;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 *
 */
public class FirstTest {
	private IEnvironment environment;
	private IMessageConsumerListener listener;
	private MyConsumer consumer;
	private MessageProducer kProducer;
	private Integer myPartition = new Integer(0);

	private final String
		GROUP_ID 	= "foo",
		PRODUCER_CLIENT_ID	= "testClient",
		CONSUMER_CLIENT_ID	= "textConsumer",
		TOPIC		= "topicmap";
	/**
	 * 
	 */
	public FirstTest() {
		environment = new MyEnvironment();
		listener = new MyListener();
		environment.logDebug("FirstTest-1");
		consumer = new MyConsumer(environment);
		environment.logDebug("FirstTest-2");
		//try {
		//	consumer.wait(1000);
		//} catch (Exception e) {
		//	environment.logError(e.getMessage(), e);
		//}
		kProducer = new MessageProducer(environment, PRODUCER_CLIENT_ID);
		environment.logDebug("FirstTest-3");
		runTest();
	}
	
	void runTest() {
		JSONObject jo = new JSONObject();
		jo.put("verb", "Hello");
		jo.put("cargo", "Hello There! "+System.currentTimeMillis());
		environment.logDebug("FirstTest.sending "+jo.toJSONString());
		kProducer.sendMessage(TOPIC, jo.toJSONString(), TOPIC, myPartition);
	}
	class MyListener implements IMessageConsumerListener {

		@Override
		public boolean acceptRecord(ConsumerRecord record) {
			try {
			environment.logDebug("GOT "+record.topic()+" | "+record.toString());
			System.out.println("GOT "+record.topic()+" "+record.toString());
			return true;
			} finally {
				consumer.close();
				kProducer.close();
			}
		}
		
	}
	
	class MyEnvironment extends RootEnvironment {

		public MyEnvironment() {
			super("kafka-props.xml", "logger.properties");
		}

		@Override
		public void shutDown() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	class MyConsumer extends StringMessageConsumer {
		private static final boolean isRewind = true;
		
		public MyConsumer(IEnvironment e) {
			super(e, CONSUMER_CLIENT_ID, TOPIC, listener, isRewind);
			
		}
		
	}

}
