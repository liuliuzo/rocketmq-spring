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

package org.apache.rocketmq.spring.core;

import java.util.List;

/**
 * A listener that consumes a batch of messages in a single invocation. The number of messages delivered to
 * {@link #onMessage(List)} is bounded by {@code consumeMessageBatchMaxSize} on
 * {@link org.apache.rocketmq.spring.annotation.RocketMQMessageListener}. The whole batch shares the same
 * consume result: if {@link #onMessage(List)} throws, every message in the batch is re-consumed later.
 */
public interface RocketMQBatchListener<T> {
    void onMessage(List<T> messages);
}
