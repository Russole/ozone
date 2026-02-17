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

package org.apache.hadoop.hdds.scm.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test the node schema loader. */
class TestYamlSchemaLoader {
  private final ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  static Stream<Arguments> getSchemaFiles() {
    return Stream.of(
        arguments("multiple-root.yaml", "Multiple root"),
        arguments("middle-leaf.yaml", "Leaf node in the middle")
    );
  }

  private String resourcePath(String resource) throws Exception {
    URL url = Objects.requireNonNull(
        classLoader.getResource(resource),
        "Test resource not found on classpath: " + resource);
    URI uri = url.toURI();
    return Paths.get(uri).toString();
  }

  @ParameterizedTest
  @MethodSource("getSchemaFiles")
  void loadSchemaFromFile(String schemaFile, String errMsg) throws Exception {
    String filePath = resourcePath("networkTopologyTestFiles/" + schemaFile);
    Throwable e = assertThrows(IllegalArgumentException.class, () ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertThat(e).hasMessageContaining(errMsg);
  }

  @Test
  void testGood() throws Exception {
    String filePath = resourcePath("networkTopologyTestFiles/good.yaml");
    assertDoesNotThrow(() ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
  }

  @Test
  void testNotExist() throws Exception {
    String filePath = resourcePath("networkTopologyTestFiles/good.yaml") + ".backup";
    Throwable e = assertThrows(FileNotFoundException.class, () ->
        NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertThat(e).hasMessageContaining("not found");
  }

  @Test
  void testDefaultYaml() throws Exception {
    String filePath = resourcePath("network-topology-default.yaml");
    NodeSchemaLoader.NodeSchemaLoadResult result =
        assertDoesNotThrow(() ->
            NodeSchemaLoader.getInstance().loadSchemaFromFile(filePath));
    assertEquals(3, result.getSchemaList().size());
  }
}
