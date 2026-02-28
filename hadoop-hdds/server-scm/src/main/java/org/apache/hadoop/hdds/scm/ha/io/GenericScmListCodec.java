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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * List codec without reflection. Element type is fixed by this codec instance.
 */
public final class GenericScmListCodec<T> implements ScmCodec<List<T>> {
  private final Class<T> elementClass;
  private final ScmCodec<T> elementCodec;

  public GenericScmListCodec(Class<T> elementClass, ScmCodec<T> elementCodec) {
    this.elementClass = elementClass;
    this.elementCodec = elementCodec;
  }

  @Override
  public ByteString serialize(List<T> values) throws InvalidProtocolBufferException {
    final ListArgument.Builder b = ListArgument.newBuilder()
        .setType(elementClass.getName());
    for (T v : values) {
      b.addValue(elementCodec.serialize(v));
    }
    return b.build().toByteString();
  }

  @Override
  public List<T> deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {

    final ListArgument listArgs = ListArgument.parseFrom(value.toByteArray());

    if (!listArgs.hasType()) {
      throw new InvalidProtocolBufferException("Missing ListArgument.type");
    }

    if (listArgs.getValueCount() == 0) {
      return new ArrayList<>();
    }

    final String expected = elementClass.getName();
    final String actual = listArgs.getType();
    if (!expected.equals(actual)) {
      throw new InvalidProtocolBufferException(
          "List element type mismatch: expected=" + expected + ", actual=" + actual);
    }

    final List<T> result = new ArrayList<>(listArgs.getValueCount());
    for (ByteString elementBytes : listArgs.getValueList()) {
      result.add(elementCodec.deserialize(elementClass, elementBytes));
    }
    return result;
  }
}
