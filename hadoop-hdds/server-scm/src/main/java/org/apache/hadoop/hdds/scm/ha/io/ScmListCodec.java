/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha.io;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ScmCodec} for {@link List} objects.
 */
public class ScmListCodec implements ScmCodec<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(ScmListCodec.class);

  /**
   * CI log 可能會把 INFO 吞掉，所以這裡用 WARN 且加固定 token 方便 grep.
   */
  private static final String TOKEN = "SCM_LIST_ELEM_TYPE";

  // Print only first N per element type to avoid flooding.
  private static final int MAX_LOG_PER_TYPE = 5;

  // One-time "I am used" marker.
  private static final AtomicBoolean HIT = new AtomicBoolean(false);

  // elementTypeName -> count
  private static final ConcurrentHashMap<String, LongAdder> ENCODE_ELEM_TYPES =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, LongAdder> DECODE_ELEM_TYPES =
      new ConcurrentHashMap<>();

  private static void logFirstN(ConcurrentHashMap<String, LongAdder> map,
      String elemType, String stage, String extra) {
    final LongAdder adder = map.computeIfAbsent(elemType, k -> new LongAdder());
    adder.increment();
    final long c = adder.longValue();

    if (c <= MAX_LOG_PER_TYPE) {
      // Use WARN to survive CI log level filters.
      LOG.warn("{} stage={} elemType={} count={} {}", TOKEN, stage, elemType, c, extra);
    } else if (c == MAX_LOG_PER_TYPE + 1) {
      LOG.warn("{} stage={} elemType={} further occurrences suppressed (>{})",
          TOKEN, stage, elemType, MAX_LOG_PER_TYPE);
    }
  }

  @Override
  public ByteString serialize(Object object) throws InvalidProtocolBufferException {

    if (HIT.compareAndSet(false, true)) {
      LOG.warn("{} HIT ScmListCodec is used (org.apache.hadoop.hdds.scm.ha.io.ScmListCodec)", TOKEN);
    }

    if (!(object instanceof List)) {
      throw new InvalidProtocolBufferException(
          "ScmListCodec.serialize expects java.util.List but got: "
              + (object == null ? "null" : object.getClass().getName()));
    }

    final ListArgument.Builder listArgs = ListArgument.newBuilder();
    final List<?> values = (List<?>) object;

    if (values.isEmpty()) {
      listArgs.setType(Object.class.getName());
      logFirstN(ENCODE_ELEM_TYPES, Object.class.getName(), "encode",
          "listSize=0 emptyList=true");
      return listArgs.build().toByteString();
    }

    final Object first = values.get(0);
    if (first == null) {
      throw new InvalidProtocolBufferException(
          "Cannot serialize List with null first element. size=" + values.size());
    }

    final Class<?> elemClass = first.getClass();
    final String elemType = elemClass.getName();
    listArgs.setType(elemType);

    logFirstN(ENCODE_ELEM_TYPES, elemType, "encode",
        "listSize=" + values.size());

    // Keep original behavior, but add debug signal if heterogeneous.
    for (int i = 0; i < values.size(); i++) {
      final Object v = values.get(i);
      if (v == null) {
        throw new InvalidProtocolBufferException(
            "Cannot serialize List with null element at index " + i
                + ", elementType=" + elemType
                + ", size=" + values.size());
      }
      if (!v.getClass().equals(elemClass)) {
        LOG.warn("{} stage=encode heterogeneousList firstElemType={} index={} actualType={} size={}",
            TOKEN, elemType, i, v.getClass().getName(), values.size());
      }

      listArgs.addValue(ScmCodecFactory.getCodec(elemClass).serialize(v));
    }

    return listArgs.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value) throws InvalidProtocolBufferException {
    try {
      if (type == null) {
        throw new InvalidProtocolBufferException("ScmListCodec.deserialize called with null type");
      }
      if (value == null) {
        throw new InvalidProtocolBufferException(
            "ScmListCodec.deserialize called with null value for type=" + type.getName());
      }

      final Class<?> concreteType = (type == List.class) ? ArrayList.class : type;

      @SuppressWarnings("unchecked")
      final List<Object> result = (List<Object>) concreteType.newInstance();

      final ListArgument listArgs;
      try {
        listArgs = (ListArgument) ReflectionUtil
            .getMethod(ListArgument.class, "parseFrom", byte[].class)
            .invoke(null, (Object) value.toByteArray());
      } catch (InvocationTargetException ite) {
        final Throwable cause = ite.getCause() == null ? ite : ite.getCause();
        final InvalidProtocolBufferException ex =
            new InvalidProtocolBufferException(
                "Failed to parse ListArgument for targetType=" + type.getName()
                    + ", concreteType=" + concreteType.getName()
                    + ", bytesLen=" + value.size()
                    + ": " + cause.getMessage());
        ex.initCause(cause);
        throw ex;
      }

      // proto2 required-equivalent check
      if (!listArgs.hasType()) {
        throw new InvalidProtocolBufferException(
            "Missing ListArgument.type for targetType=" + type.getName()
                + ", valueCount=" + listArgs.getValueCount());
      }

      final String elemType = listArgs.getType();
      logFirstN(DECODE_ELEM_TYPES, elemType, "decode",
          "targetType=" + type.getName()
              + " concreteType=" + concreteType.getName()
              + " valueCount=" + listArgs.getValueCount());

      final Class<?> dataType = ReflectionUtil.getClass(elemType);

      for (int i = 0; i < listArgs.getValueCount(); i++) {
        final ByteString element = listArgs.getValue(i);
        try {
          result.add(ScmCodecFactory.getCodec(dataType).deserialize(dataType, element));
        } catch (InvalidProtocolBufferException e) {
          final InvalidProtocolBufferException ex =
              new InvalidProtocolBufferException(
                  "Failed to decode List element at index " + i + "/" + listArgs.getValueCount()
                      + ", elementType=" + elemType
                      + ", targetType=" + type.getName()
                      + ", concreteType=" + concreteType.getName()
                      + ", elementBytesLen=" + (element == null ? -1 : element.size())
                      + ": " + e.getMessage());
          ex.initCause(e);
          throw ex;
        }
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
             IllegalAccessException | ClassNotFoundException e) {
      final InvalidProtocolBufferException ex =
          new InvalidProtocolBufferException(
              "Message cannot be decoded (targetType="
                  + (type == null ? "null" : type.getName())
                  + ", bytesLen=" + (value == null ? -1 : value.size())
                  + "): " + e.getMessage());
      ex.initCause(e);
      throw ex;
    }
  }
}
