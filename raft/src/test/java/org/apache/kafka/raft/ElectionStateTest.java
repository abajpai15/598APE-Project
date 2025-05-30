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
package org.apache.kafka.raft;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.raft.generated.QuorumStateData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ElectionStateTest {
    @Test
    void testVotedCandidateWithoutVotedId() {
        ElectionState electionState = ElectionState.withUnknownLeader(5, Set.of());
        assertFalse(electionState.isVotedCandidate(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
    }

    @Test
    void testVotedCandidateWithoutVotedDirectoryId() {
        ElectionState electionState = ElectionState.withVotedCandidate(
            5,
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Set.of()
        );
        assertTrue(electionState.isVotedCandidate(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertTrue(
            electionState.isVotedCandidate(ReplicaKey.of(1, Uuid.randomUuid()))
        );
    }

    @Test
    void testVotedCandidateWithVotedDirectoryId() {
        ReplicaKey votedKey = ReplicaKey.of(1, Uuid.randomUuid());
        ElectionState electionState = ElectionState.withVotedCandidate(
            5,
            votedKey,
            Set.of()
        );
        assertFalse(electionState.isVotedCandidate(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)));
        assertTrue(electionState.isVotedCandidate(votedKey));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testQuorumStateDataRoundTrip(short version) {
        ReplicaKey votedKey = ReplicaKey.of(1, Uuid.randomUuid());
        List<ElectionState> electionStates = List.of(
            ElectionState.withUnknownLeader(5, Set.of(1, 2, 3)),
            ElectionState.withElectedLeader(5, 1, Optional.empty(), Set.of(1, 2, 3)),
            ElectionState.withVotedCandidate(5, votedKey, Set.of(1, 2, 3)),
            ElectionState.withElectedLeader(5, 1, Optional.of(votedKey), Set.of(1, 2, 3))
        );

        final List<ElectionState> expected;
        if (version == 0) {
            expected = List.of(
                ElectionState.withUnknownLeader(5, Set.of(1, 2, 3)),
                ElectionState.withElectedLeader(5, 1, Optional.empty(), Set.of(1, 2, 3)),
                ElectionState.withVotedCandidate(
                    5,
                    ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
                    Set.of(1, 2, 3)
                ),
                ElectionState.withElectedLeader(
                    5,
                    1,
                    Optional.of(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID)),
                    Set.of(1, 2, 3)
                )
            );
        } else {
            expected = List.of(
                ElectionState.withUnknownLeader(5, Set.of()),
                ElectionState.withElectedLeader(5, 1, Optional.empty(), Set.of()),
                ElectionState.withVotedCandidate(5, votedKey, Set.of()),
                ElectionState.withElectedLeader(5, 1, Optional.of(votedKey), Set.of())
            );
        }

        int expectedId = 0;
        for (ElectionState electionState : electionStates) {
            QuorumStateData data = electionState.toQuorumStateData(version);
            assertEquals(expected.get(expectedId), ElectionState.fromQuorumStateData(data));
            expectedId++;
        }
    }
}
