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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ProduceResponse.INVALID_OFFSET;

public class ProduceRequest extends AbstractRequest {

    public static final short LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 11;

    public static Builder builder(ProduceRequestData data, boolean useTransactionV1Version) {
        // When we use transaction V1 protocol in transaction we set the request version upper limit to
        // LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 so that the broker knows that we're using transaction protocol V1.
        short maxVersion = useTransactionV1Version ?
            LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 : ApiKeys.PRODUCE.latestVersion();
        return new Builder(ApiKeys.PRODUCE.oldestVersion(), maxVersion, data);
    }

    public static Builder builder(ProduceRequestData data) {
        return builder(data, false);
    }

    public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final ProduceRequestData data;

        public Builder(short minVersion,
                       short maxVersion,
                       ProduceRequestData data) {
            super(ApiKeys.PRODUCE, minVersion, maxVersion);
            this.data = data;
        }

        @Override
        public ProduceRequest build(short version) {
            // Validate the given records first
            data.topicData().forEach(tpd ->
                tpd.partitionData().forEach(partitionProduceData ->
                    ProduceRequest.validateRecords(version, partitionProduceData.records())));
            return new ProduceRequest(data, version);
        }

        @Override
        public String toString() {
            return "(type=ProduceRequest" +
                    ", acks=" + data.acks() +
                    ", timeout=" + data.timeoutMs() +
                    ", partitionRecords=(" + data.topicData().stream().flatMap(d -> d.partitionData().stream()).collect(Collectors.toList()) +
                    "), transactionalId='" + (data.transactionalId() != null ? data.transactionalId() : "") +
                    "'";
        }
    }

    /**
     * We have to copy acks, timeout, transactionalId and partitionSizes from data since data maybe reset to eliminate
     * the reference to ByteBuffer but those metadata are still useful.
     */
    private final short acks;
    private final int timeout;
    private final String transactionalId;
    // This is set to null by `clearPartitionRecords` to prevent unnecessary memory retention when a produce request is
    // put in the purgatory (due to client throttling, it can take a while before the response is sent).
    // Care should be taken in methods that use this field.
    private volatile ProduceRequestData data;
    // the partitionSizes is lazily initialized since it is used by server-side in production.
    private volatile Map<TopicIdPartition, Integer> partitionSizes;

    public ProduceRequest(ProduceRequestData produceRequestData, short version) {
        super(ApiKeys.PRODUCE, version);
        this.data = produceRequestData;
        this.acks = data.acks();
        this.timeout = data.timeoutMs();
        this.transactionalId = data.transactionalId();
    }

    // visible for testing
    Map<TopicIdPartition, Integer> partitionSizes() {
        if (partitionSizes == null) {
            // this method may be called by different thread (see the comment on data)
            synchronized (this) {
                if (partitionSizes == null) {
                    Map<TopicIdPartition, Integer> tmpPartitionSizes = new HashMap<>();
                    data.topicData().forEach(topicData ->
                        topicData.partitionData().forEach(partitionData ->
                            // While topic id and name might not be populated at the same time in the request all the time;
                            // for example on server side they will never be populated together while in produce client they will be,
                            // to simplify initializing `TopicIdPartition` the code will use both topic name and id.
                            // TopicId will be Uuid.ZERO_UUID in versions < 13 and topic name will be used as main identifier of topic partition.
                            // TopicName will be empty string in versions >= 13 and topic id will be used as the main identifier.
                            tmpPartitionSizes.compute(new TopicIdPartition(topicData.topicId(), partitionData.index(), topicData.name()),
                                (ignored, previousValue) ->
                                    partitionData.records().sizeInBytes() + (previousValue == null ? 0 : previousValue))
                        )
                    );
                    partitionSizes = tmpPartitionSizes;
                }
            }
        }
        return partitionSizes;
    }

    /**
     * @return data or IllegalStateException if the data is removed (to prevent unnecessary memory retention).
     */
    @Override
    public ProduceRequestData data() {
        // Store it in a local variable to protect against concurrent updates
        ProduceRequestData tmp = data;
        if (tmp == null)
            throw new IllegalStateException("The partition records are no longer available because clearPartitionRecords() has been invoked.");
        return tmp;
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks)
                .append(",timeout=").append(timeout);

        if (verbose)
            bld.append(",partitionSizes=").append(Utils.mkString(partitionSizes(), "[", "]", "=", ","));
        else
            bld.append(",numPartitions=").append(partitionSizes().size());

        bld.append("}");
        return bld.toString();
    }

    @Override
    public ProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0) return null;
        ApiError apiError = ApiError.fromThrowable(e);
        ProduceResponseData data = new ProduceResponseData().setThrottleTimeMs(throttleTimeMs);
        partitionSizes().forEach((tpId, ignored) -> {
            ProduceResponseData.TopicProduceResponse tpr = data.responses().find(tpId.topic(), tpId.topicId());
            if (tpr == null) {
                tpr = new ProduceResponseData.TopicProduceResponse().setName(tpId.topic()).setTopicId(tpId.topicId());
                data.responses().add(tpr);
            }
            tpr.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(tpId.partition())
                    .setRecordErrors(Collections.emptyList())
                    .setBaseOffset(INVALID_OFFSET)
                    .setLogAppendTimeMs(RecordBatch.NO_TIMESTAMP)
                    .setLogStartOffset(INVALID_OFFSET)
                    .setErrorMessage(apiError.message())
                    .setErrorCode(apiError.error().code()));
        });
        return new ProduceResponse(data);
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, partitionSizes().size());
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public void clearPartitionRecords() {
        // lazily initialize partitionSizes.
        partitionSizes();
        data = null;
    }

    public static void validateRecords(short version, BaseRecords baseRecords) {
        if (baseRecords instanceof Records) {
            Records records = (Records) baseRecords;
            Iterator<? extends RecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext())
                throw new InvalidRecordException("Produce requests with version " + version + " must have at least " +
                        "one record batch per partition");

            RecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2)
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                        "contain record batches with magic version 2");
            if (version < 7 && entry.compressionType() == CompressionType.ZSTD) {
                throw new UnsupportedCompressionTypeException("Produce requests with version " + version + " are not allowed to " +
                        "use ZStandard compression");
            }

            if (iterator.hasNext())
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                        "contain exactly one record batch per partition");
        }
    }

    public static ProduceRequest parse(Readable readable, short version) {
        return new ProduceRequest(new ProduceRequestData(readable, version), version);
    }

    public static boolean isTransactionV2Requested(short version) {
        return version > LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2;
    }

}
