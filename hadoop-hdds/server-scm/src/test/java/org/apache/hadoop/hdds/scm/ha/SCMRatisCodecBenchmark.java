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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.PIPELINE;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmark for SCM Ratis request and response codec encode/decode.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class SCMRatisCodecBenchmark {

  @Param({"1000"})
  private int size;

  @Param({"PROTO", "LONG", "LIST_PROTO"})
  private RequestPayloadType requestPayloadType;

  private SCMRatisRequest[] requests;
  private Message[] encodedRequests;

  private Object[] responseValues;
  private Class<?>[] responseTypes;
  private RaftClientReply[] encodedResponses;

  private int index;

  @Setup
  public void setup() throws Exception {
    Random random = new Random(12345L);

    requests = new SCMRatisRequest[size];
    encodedRequests = new Message[size];

    responseValues = new Object[size];
    responseTypes = new Class<?>[size];
    encodedResponses = new RaftClientReply[size];

    for (int i = 0; i < size; i++) {
      requests[i] = createRequest(random, requestPayloadType);
      encodedRequests[i] = requests[i].encode();

      responseValues[i] = createResponseValue(random, i);
      responseTypes[i] = responseValues[i].getClass();
      encodedResponses[i] = createReply(
          SCMRatisResponse.encode(responseValues[i], responseTypes[i]));
    }
  }

  @Benchmark
  public Object encodeRequest() throws Exception {
    return requests[next()].encode();
  }

  @Benchmark
  public Object decodeRequest() throws Exception {
    return SCMRatisRequest.decode(encodedRequests[next()]);
  }

  @Benchmark
  public Object encodeResponse() throws Exception {
    int i = next();
    return SCMRatisResponse.encode(responseValues[i], responseTypes[i]);
  }

  @Benchmark
  public Object decodeResponse() throws Exception {
    return SCMRatisResponse.decode(encodedResponses[next()]);
  }

  private int next() {
    int current = index;
    index = (index + 1) % size;
    return current;
  }

  private static SCMRatisRequest createRequest(Random random, RequestPayloadType requestPayloadType) {
    switch (requestPayloadType) {
    case PROTO:
      HddsProtos.PipelineID pipelineId = PipelineID.randomId().getProtobuf();
      return SCMRatisRequest.of(PIPELINE, "benchmarkProto",
          new Class<?>[] {pipelineId.getClass()}, pipelineId);

    case LONG:
      Long value = random.nextLong();
      return SCMRatisRequest.of(PIPELINE, "benchmarkLong",
          new Class<?>[] {Long.class}, value);

    case LIST_PROTO:
      List<HddsProtos.PipelineID> pipelineIds = new ArrayList<>();
      pipelineIds.add(PipelineID.randomId().getProtobuf());
      pipelineIds.add(PipelineID.randomId().getProtobuf());
      pipelineIds.add(PipelineID.randomId().getProtobuf());
      return SCMRatisRequest.of(PIPELINE, "benchmarkList",
          new Class<?>[] {pipelineIds.getClass()}, pipelineIds);

    default:
      throw new IllegalArgumentException(
          "Unsupported request payload type: " + requestPayloadType);
    }
  }

  private static Object createResponseValue(Random random, int i) {
    return random.nextLong();
  }

  private static RaftClientReply createReply(Message message) {
    RaftGroupMemberId raftId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("peer"), RaftGroupId.randomId());

    return RaftClientReply.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(raftId)
        .setGroupId(RaftGroupId.emptyGroupId())
        .setCallId(1L)
        .setSuccess(true)
        .setMessage(message)
        .setException(null)
        .setLogIndex(1L)
        .build();
  }

  /**
   * Request payload types used by the benchmark.
   */
  public enum RequestPayloadType {
    PROTO,
    LONG,
    LIST_PROTO
  }
}
