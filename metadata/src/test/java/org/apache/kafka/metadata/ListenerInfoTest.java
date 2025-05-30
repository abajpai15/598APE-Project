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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ListenerInfoTest {
    private static final Endpoint INTERNAL = new Endpoint("INTERNAL",
        SecurityProtocol.PLAINTEXT,
        null,
        0);

    private static final Endpoint EXTERNAL = new Endpoint("EXTERNAL",
        SecurityProtocol.SASL_SSL,
        "example.com",
        9092);

    private static final Endpoint SSL = new Endpoint("SSL",
        SecurityProtocol.SSL,
        "",
        9093);

    private static final Endpoint SASL_PLAINTEXT = new Endpoint("SASL_PLAINTEXT",
        SecurityProtocol.SASL_PLAINTEXT,
        "example2.com",
        9094);

    private static final List<Endpoint> ALL = List.of(
        INTERNAL,
        EXTERNAL,
        SSL,
        SASL_PLAINTEXT);

    @Test
    public void testNullHostname() {
        assertNull(ListenerInfo.create(List.of(INTERNAL)).firstListener().host());
    }

    @Test
    public void testNullHostnameGetsResolved() throws Exception {
        assertNotNull(ListenerInfo.create(List.of(INTERNAL)).
                withWildcardHostnamesResolved().firstListener().host());
    }

    @Test
    public void testEmptyHostname() {
        assertEquals("", ListenerInfo.create(List.of(SSL)).firstListener().host());
    }

    @Test
    public void testEmptyHostnameGetsResolved() throws Exception {
        assertNotEquals("", ListenerInfo.create(List.of(SSL)).
                withWildcardHostnamesResolved().firstListener().host());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    public void testCreatePreservesOrdering(int startIndex) {
        List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < ALL.size(); i++) {
            endpoints.add(ALL.get((i + startIndex) % ALL.size()));
        }
        ListenerInfo listenerInfo = ListenerInfo.create(endpoints);
        assertEquals(ALL.get(startIndex).listener(),
            listenerInfo.firstListener().listener());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    public void testCreateWithExplicitFirstListener(int startIndex) {
        ListenerInfo listenerInfo = ListenerInfo.create(Optional.of(ALL.get(startIndex).listener()), ALL);
        assertEquals(ALL.get(startIndex).listener(),
            listenerInfo.firstListener().listener());
    }

    @Test
    public void testRoundTripToControllerRegistrationRequest() throws Exception {
        ListenerInfo listenerInfo = ListenerInfo.create(ALL).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(__ -> 9094);
        ListenerInfo newListenerInfo = ListenerInfo.fromControllerRegistrationRequest(
            listenerInfo.toControllerRegistrationRequest());
        assertEquals(listenerInfo, newListenerInfo);
    }

    @Test
    public void testToControllerRegistrationRequestFailsOnNullHost() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                toControllerRegistrationRequest());
    }

    @Test
    public void testToControllerRegistrationRequestFailsOnZeroPort() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                withWildcardHostnamesResolved().
                toControllerRegistrationRequest());
    }

    @Test
    public void testRoundTripToControllerRegistrationRecord() throws Exception {
        ListenerInfo listenerInfo = ListenerInfo.create(ALL).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(__ -> 9094);
        ListenerInfo newListenerInfo = ListenerInfo.fromControllerRegistrationRecord(
            listenerInfo.toControllerRegistrationRecord());
        assertEquals(listenerInfo, newListenerInfo);
    }

    @Test
    public void testToControllerRegistrationRecordFailsOnNullHost() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                toControllerRegistrationRecord());
    }

    @Test
    public void testToControllerRegistrationRecordFailsOnZeroPort() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                withWildcardHostnamesResolved().
                toControllerRegistrationRecord());
    }

    @Test
    public void testRoundTripToBrokerRegistrationRequest() throws Exception {
        ListenerInfo listenerInfo = ListenerInfo.create(ALL).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(__ -> 9094);
        ListenerInfo newListenerInfo = ListenerInfo.fromBrokerRegistrationRequest(
            listenerInfo.toBrokerRegistrationRequest());
        assertEquals(listenerInfo, newListenerInfo);
    }

    @Test
    public void testToBrokerRegistrationRequestFailsOnNullHost() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                toBrokerRegistrationRequest());
    }

    @Test
    public void testToBrokerRegistrationRequestFailsOnZeroPort() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                withWildcardHostnamesResolved().
                toBrokerRegistrationRequest());
    }

    @Test
    public void testRoundTripToBrokerRegistrationRecord() throws Exception {
        ListenerInfo listenerInfo = ListenerInfo.create(ALL).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(__ -> 9094);
        ListenerInfo newListenerInfo = ListenerInfo.fromBrokerRegistrationRecord(
                listenerInfo.toBrokerRegistrationRecord());
        assertEquals(listenerInfo, newListenerInfo);
    }

    @Test
    public void testToBrokerRegistrationRecordFailsOnNullHost() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                toBrokerRegistrationRecord());
    }

    @Test
    public void testToBrokerRegistrationRecordFailsOnZeroPort() {
        assertThrows(RuntimeException.class,
            () -> ListenerInfo.create(List.of(INTERNAL)).
                withWildcardHostnamesResolved().
                toBrokerRegistrationRecord());
    }

    @Test
    public void testToString() {
        ListenerInfo listenerInfo = ListenerInfo.create(List.of(EXTERNAL, SASL_PLAINTEXT));
        assertEquals("ListenerInfo(Endpoint(listenerName='EXTERNAL', securityProtocol=SASL_SSL, host='example.com', port=9092), " +
            "Endpoint(listenerName='SASL_PLAINTEXT', securityProtocol=SASL_PLAINTEXT, host='example2.com', port=9094))",
            listenerInfo.toString());
    }
}
