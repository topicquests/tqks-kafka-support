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
package org.topicquests.backside.kafka.consumer.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author jackpark
 *
 */
public interface IMessageConsumerListener {

	/**
	 * An implementation will process these records
	 * @param records
	 * @param return <code>true</code> if processing succeeds
	 */
//	boolean  acceptRecords(ConsumerRecords<String, String> records);
	
	/**
	 * Process this <code>record</code> and return <code>true</code>
	 * if successfull
	 * @param record
	 * @return
	 */
	boolean acceptRecord(ConsumerRecord record);
}