/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RequestHeaderTest {

    @Test
    public void testRequestHeaderV1() {
        short apiVersion = 1;
        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, apiVersion, "", 10);
        assertEquals(1, header.headerVersion());

        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(10, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
    }

    @Test
    public void testRequestHeaderV2() {
        short apiVersion = 2;
        RequestHeader header = new RequestHeader(ApiKeys.CREATE_DELEGATION_TOKEN, apiVersion, "", 10);
        assertEquals(2, header.headerVersion());

        ByteBuffer buffer = RequestTestUtils.serializeRequestHeader(header);
        assertEquals(11, buffer.remaining());
        RequestHeader deserialized = RequestHeader.parse(buffer);
        assertEquals(header, deserialized);
    }

    @Test
    public void parseHeaderFromBufferWithNonZeroPosition() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.position(10);

        RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, (short) 1, "", 10);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        // size must be called before write to avoid an NPE with the current implementation
        header.size(serializationCache);
        header.write(buffer, serializationCache);
        int limit = buffer.position();
        buffer.position(10);
        buffer.limit(limit);

        RequestHeader parsed = RequestHeader.parse(buffer);
        assertEquals(header, parsed);
    }

    @Test
    public void parseHeaderWithNullClientId() {
        RequestHeaderData headerData = new RequestHeaderData().
            setClientId(null).
            setCorrelationId(123).
            setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).
            setRequestApiVersion((short) 10);
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(headerData.size(serializationCache, (short) 2));
        headerData.write(new ByteBufferAccessor(buffer), serializationCache, (short) 2);
        buffer.flip();
        RequestHeader parsed = RequestHeader.parse(buffer);
        assertEquals("", parsed.clientId());
        assertEquals(123, parsed.correlationId());
        assertEquals(ApiKeys.FIND_COORDINATOR, parsed.apiKey());
        assertEquals((short) 10, parsed.apiVersion());
    }

    @Test
    public void verifySizeMethodsReturnSameValue() {
        // Create a dummy RequestHeaderData
        RequestHeaderData headerData = new RequestHeaderData().
            setClientId("hakuna-matata").
            setCorrelationId(123).
            setRequestApiKey(ApiKeys.FIND_COORDINATOR.id).
            setRequestApiVersion((short) 10);

        // Serialize RequestHeaderData to a buffer
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(headerData.size(serializationCache, (short) 2));
        headerData.write(new ByteBufferAccessor(buffer), serializationCache, (short) 2);
        buffer.flip();

        // actual call to generate the RequestHeader from buffer containing RequestHeaderData
        RequestHeader parsed = spy(RequestHeader.parse(buffer));

        // verify that the result of cached value of size is same as actual calculation of size
        int sizeCalculatedFromData = parsed.size(new ObjectSerializationCache());
        int sizeFromCache = parsed.size();
        assertEquals(sizeCalculatedFromData, sizeFromCache);

        // verify that size(ObjectSerializationCache) is only called once, i.e. during assertEquals call. This validates
        // that size() method does not calculate the size instead it uses the cached value
        verify(parsed).size(any(ObjectSerializationCache.class));
    }
}
