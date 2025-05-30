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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.VoterSet;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * A type for storing the historical value of the set of voters.
 *
 * This type can be used to keep track, in-memory, of the sets for voters stored in the latest snapshot
 * and the log segments. This is useful when generating a new snapshot at a given offset or when
 * evaluating the latest set of voters.
 */
public final class VoterSetHistory {
    private final VoterSet staticVoterSet;
    private final LogHistory<VoterSet> votersHistory = new TreeMapLogHistory<>();
    private final Logger logger;

    VoterSetHistory(VoterSet staticVoterSet, LogContext logContext) {
        this.staticVoterSet = staticVoterSet;
        this.logger = logContext.logger(getClass());
    }

    /**
     * Add a new value at a given offset.
     *
     * The provided {@code offset} must be greater than or equal to 0 and must be greater than the
     * offset of all previous calls to this method.
     *
     * @param offset the offset
     * @param voters the voters to store
     * @throws IllegalArgumentException if the offset is not greater than all previous offsets
     */
    public void addAt(long offset, VoterSet voters) {
        Optional<LogHistory.Entry<VoterSet>> lastEntry = votersHistory.lastEntry();
        if (lastEntry.isPresent() && lastEntry.get().offset() >= 0) {
            // If the last voter set comes from the replicated log then the majorities must overlap.
            // This ignores the static voter set and the bootstrapped voter set since they come from
            // the configuration and the KRaft leader never guaranteed that they are the same across
            // all replicas.
            VoterSet lastVoterSet = lastEntry.get().value();
            if (!lastVoterSet.hasOverlappingMajority(voters)) {
                logger.info(
                    "Last voter set ({}) doesn't have an overlapping majority with the new voter set ({})",
                    lastVoterSet,
                    voters
                );
            }
        }

        votersHistory.addAt(offset, voters);
    }

    /**
     * Computes the value of the voter set at a given offset.
     *
     * This function will only return values provided through {@code addAt} and it would never
     * include the {@code staticVoterSet} provided through the constructor.
     *
     * @param offset the offset (inclusive)
     * @return the voter set if one exist, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> valueAtOrBefore(long offset) {
        return votersHistory.valueAtOrBefore(offset);
    }

    /**
     * Returns the latest set of voters.
     */
    public VoterSet lastValue() {
        return votersHistory.lastEntry()
            .map(LogHistory.Entry::value)
            .orElse(staticVoterSet);
    }

    /**
     * Return the latest entry for the set of voters.
     */
    public Optional<LogHistory.Entry<VoterSet>> lastEntry() {
        return votersHistory.lastEntry();
    }

    /**
     * Returns the offset of the last voter set stored in the partition history.
     *
     * Returns {@code OptionalLong.empty} if the last voter set is from the static voters
     * configuration.
     *
     * @return the offset storing the last voter set
     */
    public OptionalLong lastVoterSetOffset() {
        return votersHistory.lastEntry()
            .map(voterSetEntry -> OptionalLong.of(voterSetEntry.offset()))
            .orElseGet(OptionalLong::empty);
    }

    /**
     * Removes all entries with an offset greater than or equal to {@code endOffset}.
     *
     * @param endOffset the ending offset
     */
    public void truncateNewEntries(long endOffset) {
        votersHistory.truncateNewEntries(endOffset);
    }

    /**
     * Removes all entries but the last entry that has an offset that is less than or equal to
     * {@code startOffset}.
     *
     * This operation does not remove the entry with the largest offset that is less than or equal
     * to {@code startOffset}. This is needed so that calls to {@code valueAtOrBefore} and
     * {@code lastEntry} always return a non-empty value if a value was previously added to this
     * object.
     *
     * @param startOffset the starting offset
     */
    public void truncateOldEntries(long startOffset) {
        votersHistory.truncateOldEntries(startOffset);
    }

    /**
     * Removes all the values from this object.
     */
    public void clear() {
        votersHistory.clear();
    }
}
