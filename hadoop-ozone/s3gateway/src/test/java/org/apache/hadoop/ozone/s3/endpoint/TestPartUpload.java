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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.util.Arrays.asList;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.FailingInputStream;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.stream.Stream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * This class tests Upload part request.
 */
@ParameterizedClass
@ValueSource(booleans = {false, true})
public class TestPartUpload {

  private ObjectEndpoint rest;
  private OzoneClient client;

  private HttpHeaders headers;

  @Parameter
  private boolean enableDataStream;

  @BeforeEach
  public void setUp() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, enableDataStream);

    rest = spy(EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .setConfig(conf)
        .build());
    assertEquals(enableDataStream, rest.isDatastreamEnabled());
  }

  @Test
  public void testPartUpload() throws Exception {
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    String content = "Multipart Upload";
    String eTag;
    try (Response response = put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content)) {
      eTag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(eTag);
    }
    assertContentLength(uploadID, OzoneConsts.KEY, content.length());

    // Upload part again with same part Number, the ETag should be changed.
    String newContent = "Multipart Upload Changed";
    try (Response response = put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, newContent)) {
      String newETag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(newETag);
      assertNotEquals(eTag, newETag);
    }
  }

  @Test
  public void testLargeFileMultipartUpload(@TempDir Path tempDir) throws Exception {
    assumeFalse(enableDataStream);
    String keyName = UUID.randomUUID().toString();

    // Step 1: Start the MPU session.  This corresponds to the AWS S3
    // CreateMultipartUpload API and returns the upload ID used to bind all
    // following part uploads to the same final object.
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET,
        keyName);

    // Step 2: Prepare two real files as MPU part bodies.  Each file is 5MB,
    // which is the minimum non-final part size required by S3 semantics.
    // Distinct byte values make the final object ordering easy to verify:
    // part 1 should become the first 5MB, part 2 the next 5MB.
    int partSize = (int) (5 * OzoneConsts.MB);
    Path firstPartFile = tempDir.resolve("multipart-upload-part-1.bin");
    Path secondPartFile = tempDir.resolve("multipart-upload-part-2.bin");
    createFile(firstPartFile, partSize, (byte) 7);
    createFile(secondPartFile, partSize, (byte) 8);

    // Step 3: Upload each part independently.  uploadFilePart opens the file
    // as the request body, calls ObjectEndpoint.put with uploadId and
    // partNumber query parameters, and returns the partNumber + ETag pair
    // required by CompleteMultipartUpload.
    CompleteMultipartUploadRequest.Part firstPart = uploadFilePart(keyName,
        uploadID, 1, partSize, firstPartFile);
    CompleteMultipartUploadRequest.Part secondPart = uploadFilePart(keyName,
        uploadID, 2, partSize, secondPartFile);

    // Step 4: Complete the MPU by submitting the manifest of uploaded parts.
    // The endpoint validates the part numbers and ETags, then materializes the
    // final object from the previously uploaded part data.
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, keyName, uploadID,
        asList(firstPart, secondPart));

    // Step 5: Read the committed object through the Ozone client and verify
    // both the visible object size and the final byte ordering.
    OzoneBucket bucket = client.getObjectStore()
        .getS3Bucket(OzoneConsts.S3_BUCKET);
    assertEquals(2L * partSize, bucket.getKey(keyName).getDataSize());
    assertPartContent(bucket, keyName, partSize, (byte) 7, (byte) 8);
  }

  @Test
  public void testPartUploadWithStandardIA() throws Exception {
    when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn(S3StorageType.STANDARD_IA.name(), (String)null);
    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, keyName);

    String content = "Multipart Upload";
    try (Response response = put(rest, OzoneConsts.S3_BUCKET, keyName, 1, uploadID, content)) {
      assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
      assertEquals(200, response.getStatus());
    }
    assertContentLength(uploadID, keyName, content.length());
  }

  @Test
  public void testPartUploadWithIncorrectUploadID() {
    assertErrorResponse(S3ErrorTable.NO_SUCH_UPLOAD,
        () -> put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, "random", "any"));
  }

  @Test
  public void testPartUploadRejectsIncompleteBody() throws Exception {
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    String content = "Multipart Upload";

    assertErrorResponse(S3ErrorTable.INVALID_REQUEST,
        () -> put(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
            1, uploadID, content.length() + 1, content));
    OzoneMultipartUploadPartListParts parts =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET)
            .listParts(OzoneConsts.KEY, uploadID, 0, 100);
    assertEquals(0, parts.getPartInfoList().size());
  }

  @Test
  public void testPartUploadStreamContentLength()
      throws IOException, OS3Exception {
    String keyName = UUID.randomUUID().toString();

    int contentLength = 15;
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn(String.valueOf(contentLength));

    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, keyName);

    assertSucceeds(() -> put(rest, OzoneConsts.S3_BUCKET, keyName, 1, uploadID, chunkedContent));
    assertContentLength(uploadID, keyName, contentLength);
  }

  @Test
  public void testPartUploadMessageDigestResetDuringException() throws IOException, OS3Exception {
    String uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    MessageDigest messageDigest = mock(MessageDigest.class);
    when(messageDigest.getAlgorithm()).thenReturn(OzoneConsts.MD5_HASH);
    MessageDigest sha256Digest = mock(MessageDigest.class);
    when(sha256Digest.getAlgorithm()).thenReturn(OzoneConsts.FILE_HASH);
    try (MockedStatic<EndpointBase> endpoint =
        mockStatic(EndpointBase.class, CALLS_REAL_METHODS)) {
      // Add the mocked methods only during part upload
      endpoint.when(EndpointBase::getMD5DigestInstance).thenReturn(messageDigest);
      endpoint.when(EndpointBase::getSha256DigestInstance).thenReturn(sha256Digest);

      String content = "Multipart Upload";
      try (InputStream body = new FailingInputStream(
          content.getBytes(StandardCharsets.UTF_8), 5)) {
        IOException ex = assertThrows(IOException.class,
            () -> putPart(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1,
                uploadID, content.length(), body).close());
        assertEquals("upload interrupted", ex.getMessage());
      }

      // Verify that the message digest is reset so that the instance can be reused for the
      // next request in the same thread
      verify(messageDigest, times(1)).reset();
      verify(sha256Digest, times(1)).reset();
    }
  }

  @Test
  public void testPartUploadWithContentMD5() throws Exception {
    String content = "Multipart Upload Part";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    byte[] md5Bytes = MessageDigest.getInstance("MD5").digest(contentBytes);
    String md5Base64 = Base64.getEncoder().encodeToString(md5Bytes);

    HttpHeaders headersWithMD5 = mock(HttpHeaders.class);
    when(headersWithMD5.getHeaderString("Content-MD5")).thenReturn(md5Base64);
    when(headersWithMD5.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithMD5.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headersWithMD5)
        .setClient(client)
        .build();

    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    try (Response response = put(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content)) {
      assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
      assertEquals(200, response.getStatus());
    }

    assertContentLength(uploadID, OzoneConsts.KEY, content.length());
  }

  public static Stream<Arguments> wrongContentMD5Provider() throws NoSuchAlgorithmException {
    byte[] wrongContentBytes = "wrong".getBytes(StandardCharsets.UTF_8);
    byte[] wrongMd5Bytes = MessageDigest.getInstance("MD5").digest(wrongContentBytes);
    String wrongMd5Base64 = Base64.getEncoder().encodeToString(wrongMd5Bytes);
    return Stream.of(
        Arguments.arguments(wrongMd5Base64, S3ErrorTable.BAD_DIGEST),
        Arguments.arguments("invalid-base64", S3ErrorTable.INVALID_DIGEST)
    );
  }

  @ParameterizedTest
  @MethodSource("wrongContentMD5Provider")
  public void testPartUploadWithWrongContentMD5(String wrongContentMD5, S3ErrorTable s3Error) throws Exception {
    String content = "Multipart Upload Part";

    HttpHeaders headersWithWrongMD5 = mock(HttpHeaders.class);
    when(headersWithWrongMD5.getHeaderString("Content-MD5")).thenReturn(wrongContentMD5);
    when(headersWithWrongMD5.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithWrongMD5.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headersWithWrongMD5)
        .setClient(client)
        .build();

    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    assertErrorResponse(s3Error,
        () -> put(endpoint, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, content));
  }

  private CompleteMultipartUploadRequest.Part uploadFilePart(String keyName,
      String uploadID, int partNumber, int contentLength, Path file)
      throws IOException, OS3Exception {
    // CompleteMultipartUpload does not resend object bytes.  It sends a
    // manifest containing each PartNumber and the ETag returned by UploadPart,
    // so this helper builds that manifest entry while uploading the part.
    CompleteMultipartUploadRequest.Part part =
        new CompleteMultipartUploadRequest.Part();

    // This file stream is the UploadPart request body.  putPart wires the
    // request as:
    //   PUT /bucket/key?uploadId=<uploadID>&partNumber=<partNumber>
    // ObjectEndpoint.put then dispatches to createMultipartKey, where the
    // bytes are copied into an OzoneOutputStream for this MPU part.
    try (InputStream body = Files.newInputStream(file);
         Response response = putPart(rest, OzoneConsts.S3_BUCKET, keyName,
             partNumber, uploadID, contentLength, body)) {
      assertEquals(200, response.getStatus());

      // UploadPart must return an ETag.  The complete request uses this ETag
      // to prove that the manifest references the exact part accepted by the
      // server.
      String eTag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(eTag);
      part.setETag(eTag);
      part.setPartNumber(partNumber);
    }

    // Check the in-progress MPU state before completion.  This verifies that
    // the upload was recorded as a part of the upload ID, not merely accepted
    // as a successful HTTP response.
    assertContentLength(uploadID, keyName, contentLength, partNumber);
    return part;
  }

  private static void assertPartContent(OzoneBucket bucket, String keyName,
      int partSize, byte... expectedValues) throws IOException {
    // After completion, the object must be readable as one contiguous key.
    // Reading fixed-size windows lets the test prove that complete preserved
    // the requested part order.
    try (InputStream input = bucket.readKey(keyName)) {
      byte[] actualPart = new byte[partSize];
      for (byte expectedValue : expectedValues) {
        IOUtils.readFully(input, actualPart);
        byte[] expectedPart = new byte[partSize];
        Arrays.fill(expectedPart, expectedValue);
        assertArrayEquals(expectedPart, actualPart);
      }
      assertEquals(-1, input.read());
    }
  }

  private static void createFile(Path file, int size, byte value)
      throws IOException {
    byte[] buffer = new byte[8192];
    Arrays.fill(buffer, value);
    int remaining = size;
    try (OutputStream output = Files.newOutputStream(file)) {
      while (remaining > 0) {
        int bytesToWrite = Math.min(remaining, buffer.length);
        output.write(buffer, 0, bytesToWrite);
        remaining -= bytesToWrite;
      }
    }
  }

  private void assertContentLength(String uploadID, String key,
      long contentLength) throws IOException {
    assertContentLength(uploadID, key, contentLength, 1);
  }

  private void assertContentLength(String uploadID, String key,
      long contentLength, int partNumber) throws IOException {
    OzoneMultipartUploadPartListParts parts =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET)
            .listParts(key, uploadID, 0, 100);
    assertEquals(partNumber, parts.getPartInfoList().size());
    assertEquals(contentLength,
        parts.getPartInfoList().get(partNumber - 1).getSize());
  }

  private static Response putPart(ObjectEndpoint subject, String bucket,
      String key, int partNumber, String uploadID, long contentLength,
      InputStream body) throws IOException, OS3Exception {
    // These query parameters are what distinguish UploadPart from a normal
    // PutObject request.  Without them, ObjectEndpoint.put would follow the
    // regular create-key path instead of the multipart createMultipartKey path.
    subject.queryParamsForTest().set(S3Consts.QueryParams.UPLOAD_ID, uploadID);
    subject.queryParamsForTest().setInt(S3Consts.QueryParams.PART_NUMBER,
        partNumber);

    // Mock the HTTP method and Content-Length normally provided by the JAX-RS
    // request.  createMultipartKey uses Content-Length as the expected part
    // size while copying the request body to the OzoneOutputStream.
    when(subject.getContext().getMethod()).thenReturn(HttpMethod.PUT);
    when(subject.getHeaders().getHeaderString(HttpHeaders.CONTENT_LENGTH))
        .thenReturn(String.valueOf(contentLength));
    return subject.put(bucket, key, body);
  }
}
