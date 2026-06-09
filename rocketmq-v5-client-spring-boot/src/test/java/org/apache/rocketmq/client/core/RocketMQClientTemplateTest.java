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
package org.apache.rocketmq.client.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RocketMQClientTemplateTest {

    private static final String DESTINATION = "test-topic:tagA";

    private List<Message<?>> buildMessages(int size) {
        List<Message<?>> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messages.add(MessageBuilder.withPayload("payload" + i).build());
        }
        return messages;
    }

    @Test
    public void testSyncSendBatch() throws Exception {
        Producer producer = mock(Producer.class);
        SendReceipt sendReceipt = mock(SendReceipt.class);
        when(producer.send(any())).thenReturn(sendReceipt);

        RocketMQClientTemplate template = new RocketMQClientTemplate();
        template.setProducer(producer);

        List<SendReceipt> sendReceipts = template.syncSendBatch(DESTINATION, buildMessages(3));

        assertThat(sendReceipts).hasSize(3);
        verify(producer, times(3)).send(any());
    }

    @Test
    public void testSyncSendBatchSkipsEmptyMessage() throws Exception {
        Producer producer = mock(Producer.class);
        SendReceipt sendReceipt = mock(SendReceipt.class);
        when(producer.send(any())).thenReturn(sendReceipt);

        RocketMQClientTemplate template = new RocketMQClientTemplate();
        template.setProducer(producer);

        List<Message<?>> messages = buildMessages(2);
        messages.add(null);

        List<SendReceipt> sendReceipts = template.syncSendBatch(DESTINATION, messages);

        assertThat(sendReceipts).hasSize(2);
        verify(producer, times(2)).send(any());
    }

    @Test
    public void testAsyncSendBatch() throws Exception {
        Producer producer = mock(Producer.class);
        SendReceipt sendReceipt = mock(SendReceipt.class);
        when(producer.sendAsync(any())).thenReturn(CompletableFuture.completedFuture(sendReceipt));

        RocketMQClientTemplate template = new RocketMQClientTemplate();
        template.setProducer(producer);

        List<SendReceipt> sendReceipts = template.asyncSendBatch(DESTINATION, buildMessages(3)).get();

        assertThat(sendReceipts).hasSize(3);
        verify(producer, times(3)).sendAsync(any());
    }

    @Test
    public void testSyncSendBatchWithEmptyCollection() {
        RocketMQClientTemplate template = new RocketMQClientTemplate();
        assertThatThrownBy(() -> template.syncSendBatch(DESTINATION, Collections.emptyList()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAsyncSendBatchWithEmptyCollection() {
        RocketMQClientTemplate template = new RocketMQClientTemplate();
        assertThatThrownBy(() -> template.asyncSendBatch(DESTINATION, Collections.emptyList()))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
