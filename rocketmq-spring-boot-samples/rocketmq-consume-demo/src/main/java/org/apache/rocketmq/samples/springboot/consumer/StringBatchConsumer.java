/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.samples.springboot.consumer;

import java.util.List;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQBatchListener;
import org.springframework.stereotype.Service;

/**
 * StringBatchConsumer consumes messages in batches. The consumeMessageBatchMaxSize property bounds how
 * many messages are delivered to onMessage in a single invocation; the whole batch shares one consume
 * result, so if onMessage throws, every message in the batch is re-consumed later.
 */
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.topic}", consumerGroup = "string_batch_consumer", selectorExpression = "${demo.rocketmq.tag}", consumeMessageBatchMaxSize = 10)
public class StringBatchConsumer implements RocketMQBatchListener<String> {
    @Override
    public void onMessage(List<String> messages) {
        System.out.printf("------- StringBatchConsumer received %d messages: %s \n", messages.size(), messages);
    }
}
