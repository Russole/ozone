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

import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Maps types to the corresponding {@link ScmCodec} implementation.
 */
public final class ScmCodecFactory {

  private static Map<Class<?>, ScmCodec<?>> codecs = new HashMap<>();
  private static Map<Type, ScmCodec<?>> listCodecs = new HashMap<>();

  static {
    putProto(ContainerID.getDefaultInstance());
    putProto(PipelineID.getDefaultInstance());
    putProto(Pipeline.getDefaultInstance());
    putProto(ContainerInfoProto.getDefaultInstance());
    putProto(DeletedBlocksTransaction.getDefaultInstance());
    putProto(DeletedBlocksTransactionSummary.getDefaultInstance());

    codecs.put(Integer.class, new ScmIntegerCodec());
    codecs.put(Long.class, new ScmLongCodec());
    codecs.put(String.class, new ScmStringCodec());
    codecs.put(Boolean.class, new ScmBooleanCodec());
    codecs.put(BigInteger.class, new ScmBigIntegerCodec());
    codecs.put(X509Certificate.class, new ScmX509CertificateCodec());
    codecs.put(com.google.protobuf.ByteString.class, new ScmNonShadedByteStringCodec());
    codecs.put(ByteString.class, new ScmByteStringCodec());
    codecs.put(ManagedSecretKey.class, new ScmManagedSecretKeyCodec());

    putEnum(LifeCycleEvent.class, LifeCycleEvent::forNumber);
    putEnum(PipelineState.class, PipelineState::forNumber);
    putEnum(NodeType.class, NodeType::forNumber);

    putListCodec(DeletedBlocksTransaction.class);
    putListCodec(Long.class);
    putListCodec(ManagedSecretKey.class);
    putListCodec(X509Certificate.class);
  }

  static <T extends Message> void putProto(T proto) {
    codecs.put(proto.getClass(),
        new ScmNonShadedGeneratedMessageCodec<>(proto.getParserForType()));
  }

  static <T extends Enum<T> & ProtocolMessageEnum> void putEnum(
      Class<T> enumClass, IntFunction<T> forNumber) {
    codecs.put(enumClass, new ScmEnumCodec<>(enumClass, forNumber));
  }

  private static <T> void putListCodec(Class<T> elementType) {
    @SuppressWarnings("unchecked")
    ScmCodec<T> elementCodec = (ScmCodec<T>) codecs.get(elementType);
    if (elementCodec == null) {
      throw new IllegalStateException(
          "Element codec must be registered before list codec: " + elementType);
    }
    listCodecs.put(elementType, new ScmListCodec<>(elementType, elementCodec));
  }

  private ScmCodecFactory() { }

  public static ScmCodec<?> getCodec(Class<?> type, Type genericType)
      throws InvalidProtocolBufferException {
    if (List.class.isAssignableFrom(type)) {
      return getListCodec(genericType);
    }
    return getCodec(type);
  }

  public static ScmCodec getCodec(Class<?> type)
      throws InvalidProtocolBufferException {
    final List<Class<?>> classes = new ArrayList<>();
    classes.add(type);
    classes.addAll(ClassUtils.getAllSuperclasses(type));
    classes.addAll(ClassUtils.getAllInterfaces(type));
    for (Class<?> clazz : classes) {
      if (codecs.containsKey(clazz)) {
        return codecs.get(clazz);
      }
    }
    throw new InvalidProtocolBufferException(
        "Codec for " + type + " not found!");
  }

  private static ScmCodec<?> getListCodec(Type genericType)
      throws InvalidProtocolBufferException {
    if (!(genericType instanceof ParameterizedType)) {
      throw new InvalidProtocolBufferException(
          "Missing generic type information for List parameter: " + genericType);
    }

    ParameterizedType parameterizedType = (ParameterizedType) genericType;
    Type elementType = parameterizedType.getActualTypeArguments()[0];
    if (!(elementType instanceof Class<?>)) {
      throw new InvalidProtocolBufferException(
          "Unsupported List element type: " + elementType);
    }

    Class<?> elementClass = (Class<?>) elementType;
    ScmCodec<?> listCodec = listCodecs.get(elementClass);
    if (listCodec != null) {
      return listCodec;
    }

    throw new InvalidProtocolBufferException(
        "List codec for element type " + elementClass.getName()
            + " not found!");
  }
}
