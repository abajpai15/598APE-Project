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

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public final class MessageUtil {

    public static final long UNSIGNED_INT_MAX = 4294967295L;

    public static final int UNSIGNED_SHORT_MAX = 65535;

    /**
     * Copy a byte buffer into an array.  This will not affect the buffer's
     * position or mark.
     */
    public static byte[] byteBufferToArray(ByteBuffer buf) {
        byte[] arr = new byte[buf.remaining()];
        int prevPosition = buf.position();
        try {
            buf.get(arr);
        } finally {
            buf.position(prevPosition);
        }
        return arr;
    }

    public static String deepToString(Iterator<?> iter) {
        StringBuilder bld = new StringBuilder("[");
        String prefix = "";
        while (iter.hasNext()) {
            Object object = iter.next();
            bld.append(prefix);
            bld.append(object.toString());
            prefix = ", ";
        }
        bld.append("]");
        return bld.toString();
    }

    public static byte jsonNodeToByte(JsonNode node, String about) {
        int value = jsonNodeToInt(node, about);
        if (value > Byte.MAX_VALUE) {
            if (value <= 256) {
                // It's more traditional to refer to bytes as unsigned,
                // so we support that here.
                value -= 128;
            } else {
                throw new RuntimeException(about + ": value " + value +
                    " does not fit in an 8-bit signed integer.");
            }
        }
        if (value < Byte.MIN_VALUE) {
            throw new RuntimeException(about + ": value " + value +
                " does not fit in an 8-bit signed integer.");
        }
        return (byte) value;
    }

    public static short jsonNodeToShort(JsonNode node, String about) {
        int value = jsonNodeToInt(node, about);
        if ((value < Short.MIN_VALUE) || (value > Short.MAX_VALUE)) {
            throw new RuntimeException(about + ": value " + value +
                " does not fit in a 16-bit signed integer.");
        }
        return (short) value;
    }

    public static int jsonNodeToUnsignedShort(JsonNode node, String about) {
        int value = jsonNodeToInt(node, about);
        if (value < 0 || value > UNSIGNED_SHORT_MAX) {
            throw new RuntimeException(about + ": value " + value +
                " does not fit in a 16-bit unsigned integer.");
        }
        return value;
    }

    public static long jsonNodeToUnsignedInt(JsonNode node, String about) {
        long value = jsonNodeToLong(node, about);
        if (value < 0 || value > UNSIGNED_INT_MAX) {
            throw new RuntimeException(about + ": value " + value +
                    " does not fit in a 32-bit unsigned integer.");
        }
        return value;
    }

    public static int jsonNodeToInt(JsonNode node, String about) {
        if (node.isInt()) {
            return node.asInt();
        }
        if (node.isTextual()) {
            throw new NumberFormatException(about + ": expected an integer or " +
                "string type, but got " + node.getNodeType());
        }
        String text = node.asText();
        if (text.startsWith("0x")) {
            try {
                return Integer.parseInt(text.substring(2), 16);
            } catch (NumberFormatException e) {
                throw new NumberFormatException(about + ": failed to " +
                    "parse hexadecimal number: " + e.getMessage());
            }
        } else {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException e) {
                throw new NumberFormatException(about + ": failed to " +
                    "parse number: " + e.getMessage());
            }
        }
    }

    public static long jsonNodeToLong(JsonNode node, String about) {
        if (node.isLong()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            throw new NumberFormatException(about + ": expected an integer or " +
                "string type, but got " + node.getNodeType());
        }
        String text = node.asText();
        if (text.startsWith("0x")) {
            try {
                return Long.parseLong(text.substring(2), 16);
            } catch (NumberFormatException e) {
                throw new NumberFormatException(about + ": failed to " +
                    "parse hexadecimal number: " + e.getMessage());
            }
        } else {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e) {
                throw new NumberFormatException(about + ": failed to " +
                    "parse number: " + e.getMessage());
            }
        }
    }

    public static byte[] jsonNodeToBinary(JsonNode node, String about) {
        try {
            byte[] value = node.binaryValue();
            if (value == null) {
                throw new IllegalArgumentException(about + ": expected Base64-encoded binary data.");
            }

            return value;
        } catch (IOException e) {
            throw new UncheckedIOException(about + ": unable to retrieve Base64-encoded binary data", e);
        }
    }

    public static double jsonNodeToDouble(JsonNode node, String about) {
        if (!node.isFloatingPointNumber()) {
            throw new NumberFormatException(about + ": expected a floating point " +
                "type, but got " + node.getNodeType());
        }
        return node.asDouble();
    }

    public static byte[] duplicate(byte[] array) {
        if (array == null)
            return null;
        return Arrays.copyOf(array, array.length);
    }

    /**
     * Compare two RawTaggedFields lists.
     * A null list is equivalent to an empty one in this context.
     */
    public static boolean compareRawTaggedFields(List<RawTaggedField> first,
                                                 List<RawTaggedField> second) {
        if (first == null) {
            return second == null || second.isEmpty();
        } else if (second == null) {
            return first.isEmpty();
        } else {
            return first.equals(second);
        }
    }

    public static ByteBufferAccessor toByteBufferAccessor(final Message message, final short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, version);
        ByteBufferAccessor bytes = new ByteBufferAccessor(ByteBuffer.allocate(messageSize));
        message.write(bytes, cache, version);
        bytes.flip();
        return bytes;
    }

    public static ByteBuffer toVersionPrefixedByteBuffer(final short version, final Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, version);
        ByteBufferAccessor bytes = new ByteBufferAccessor(ByteBuffer.allocate(messageSize + 2));
        bytes.writeShort(version);
        message.write(bytes, cache, version);
        bytes.flip();
        return bytes.buffer();
    }

    public static byte[] toVersionPrefixedBytes(final short version, final Message message) {
        ByteBuffer buffer = toVersionPrefixedByteBuffer(version, message);
        // take the inner array directly if it is full of data.
        if (buffer.hasArray() &&
            buffer.arrayOffset() == 0 &&
            buffer.position() == 0 &&
            buffer.limit() == buffer.array().length) return buffer.array();
        else return Utils.toArray(buffer);
    }

    public static ByteBuffer toCoordinatorTypePrefixedByteBuffer(final ApiMessage message) {
        if (message.apiKey() < 0) {
            throw new IllegalArgumentException("Cannot serialize a message without an api key.");
        }
        if (message.highestSupportedVersion() != 0 || message.lowestSupportedVersion() != 0) {
            throw new IllegalArgumentException("Cannot serialize a message with a different version than 0.");
        }

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, (short) 0);
        ByteBufferAccessor bytes = new ByteBufferAccessor(ByteBuffer.allocate(messageSize + 2));
        bytes.writeShort(message.apiKey());
        message.write(bytes, cache, (short) 0);
        bytes.flip();
        return bytes.buffer();
    }

    public static byte[] toCoordinatorTypePrefixedBytes(final ApiMessage message) {
        ByteBuffer buffer = toCoordinatorTypePrefixedByteBuffer(message);
        // take the inner array directly if it is full of data.
        if (buffer.hasArray() &&
            buffer.arrayOffset() == 0 &&
            buffer.position() == 0 &&
            buffer.limit() == buffer.array().length) return buffer.array();
        else return Utils.toArray(buffer);
    }
}
