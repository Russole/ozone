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

  // elementTypeName -> count
  private static final ConcurrentHashMap<String, LongAdder> ENCODE_ELEM_TYPES =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, LongAdder> DECODE_ELEM_TYPES =
      new ConcurrentHashMap<>();

  private static void logFirstN(ConcurrentHashMap<String, LongAdder> map,
      String key, int n, String msg) {
    final LongAdder adder = map.computeIfAbsent(key, k -> new LongAdder());
    adder.increment();
    final long c = adder.longValue();
    if (c <= n) {
      LOG.info("{} (count={})", msg, c);
    } else if (c == n + 1) {
      LOG.info("Further occurrences for elementType=[{}] will be suppressed (count>{}).", key, n);
    }
  }

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {

    if (!(object instanceof List)) {
      throw new InvalidProtocolBufferException(
          "ScmListCodec.serialize expects java.util.List but got: "
              + (object == null ? "null" : object.getClass().getName()));
    }

    final ListArgument.Builder listArgs = ListArgument.newBuilder();
    final List<?> values = (List<?>) object;

    if (!values.isEmpty()) {
      Object first = values.get(0);
      if (first == null) {
        throw new InvalidProtocolBufferException(
            "Cannot serialize List with null first element. size=" + values.size());
      }

      Class<?> type = first.getClass();
      listArgs.setType(type.getName());

      logFirstN(ENCODE_ELEM_TYPES, type.getName(), 3,
          "SCM ListCodec encode: elementType=" + type.getName()
              + ", listSize=" + values.size());

      for (int i = 0; i < values.size(); i++) {
        Object value = values.get(i);
        if (value == null) {
          throw new InvalidProtocolBufferException(
              "Cannot serialize List with null element at index " + i
                  + ", elementType=" + type.getName()
                  + ", size=" + values.size());
        }
        // keep original behavior (no heterogeneous check), just log if differs
        if (!value.getClass().equals(type)) {
          LOG.warn("SCM ListCodec encode: heterogeneous element detected. "
                  + "Declared elementType from first={}, index={}, actual={}, size={}",
              type.getName(), i, value.getClass().getName(), values.size());
        }
        listArgs.addValue(ScmCodecFactory.getCodec(type).serialize(value));
      }
    } else {
      listArgs.setType(Object.class.getName());
      logFirstN(ENCODE_ELEM_TYPES, Object.class.getName(), 3,
          "SCM ListCodec encode: empty list (type=Object), listSize=0");
    }

    return listArgs.build().toByteString();
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      if (type == null) {
        throw new InvalidProtocolBufferException("ScmListCodec.deserialize called with null type");
      }
      if (value == null) {
        throw new InvalidProtocolBufferException(
            "ScmListCodec.deserialize called with null value for type=" + type.getName());
      }

      // If argument type is the generic interface, then determine a concrete implementation.
      Class<?> concreteType = (type == List.class) ? ArrayList.class : type;

      @SuppressWarnings("unchecked")
      List<Object> result = (List<Object>) concreteType.newInstance();

      final ListArgument listArgs;
      try {
        listArgs = (ListArgument) ReflectionUtil
            .getMethod(ListArgument.class, "parseFrom", byte[].class)
            .invoke(null, (Object) value.toByteArray());
      } catch (InvocationTargetException ite) {
        Throwable cause = ite.getCause() == null ? ite : ite.getCause();
        InvalidProtocolBufferException ex =
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

      final String elemTypeName = listArgs.getType();
      logFirstN(DECODE_ELEM_TYPES, elemTypeName, 3,
          "SCM ListCodec decode: elementType=" + elemTypeName
              + ", targetType=" + type.getName()
              + ", valueCount=" + listArgs.getValueCount());

      final Class<?> dataType = ReflectionUtil.getClass(elemTypeName);
      for (int i = 0; i < listArgs.getValueCount(); i++) {
        ByteString element = listArgs.getValue(i);
        try {
          result.add(ScmCodecFactory.getCodec(dataType)
              .deserialize(dataType, element));
        } catch (InvalidProtocolBufferException e) {
          throw new InvalidProtocolBufferException(
              "Failed to decode List element at index " + i + "/" + listArgs.getValueCount()
                  + ", elementType=" + elemTypeName
                  + ", targetType=" + type.getName()
                  + ", concreteType=" + concreteType.getName()
                  + ", elementBytesLen=" + (element == null ? -1 : element.size())
                  + ": " + e.getMessage(), e);
        }
      }
      return result;
    } catch (InstantiationException | NoSuchMethodException |
             IllegalAccessException | ClassNotFoundException ex) {
      throw new InvalidProtocolBufferException(
          "Message cannot be decoded (targetType=" + (type == null ? "null" : type.getName())
              + ", bytesLen=" + (value == null ? -1 : value.size()) + "): "
              + ex.getMessage(), ex);
    }
  }
}
