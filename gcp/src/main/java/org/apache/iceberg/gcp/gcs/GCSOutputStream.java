/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.gcs;

import com.google.api.client.util.Lists;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GCSOutputStream leverages native streaming channels from the GCS API for streaming uploads.
 * See <a href="https://cloud.google.com/storage/docs/streaming">Streaming Transfers</a>
 */
class GCSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;

  private OutputStream stream;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSOutputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics)
      throws IOException {
    LOG.info("[TEMP-DEBUG] GCSOutputStream creation called for: gen:{}, bkt:{}, nm:{}    lobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;

    createStack = Thread.currentThread().getStackTrace();

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    LOG.info("[TEMP-DEBUG] About to openStream for GCSOutputStream creation called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);

    openStream();

    LOG.info("[TEMP-DEBUG] Stream created for GCSOutputStream creation called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {

    LOG.info("[TEMP-DEBUG] flush() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    stream.flush();

    LOG.info("[TEMP-DEBUG] flush() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  @Override
  public void write(int b) throws IOException {

    LOG.info("[TEMP-DEBUG] write() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    stream.write(b);
    pos += 1;
    writeBytes.increment();
    writeOperations.increment();

    LOG.info("[TEMP-DEBUG] write() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {

    LOG.info("[TEMP-DEBUG] write2() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    stream.write(b, off, len);
    pos += len;
    writeBytes.increment(len);
    writeOperations.increment();

    LOG.info("[TEMP-DEBUG] write2() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  private void openStream() {

    LOG.info("[TEMP-DEBUG] openStream() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    List<BlobWriteOption> writeOptions = Lists.newArrayList();

    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    WriteChannel channel =
        storage.writer(
            BlobInfo.newBuilder(blobId).build(), writeOptions.toArray(new BlobWriteOption[0]));

    gcpProperties.channelWriteChunkSize().ifPresent(channel::setChunkSize);

    stream = Channels.newOutputStream(channel);

    LOG.info("[TEMP-DEBUG] openStream() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  @Override
  public void close() throws IOException {

    LOG.info("[TEMP-DEBUG] close() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    if (closed) {
      return;
    }

    super.close();
    closed = true;
    stream.close();

    LOG.info("[TEMP-DEBUG] close() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {

    LOG.info("[TEMP-DEBUG] finalize() called for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }

    LOG.info("[TEMP-DEBUG] finalize() done for: gen:{}, bkt:{}, nm:{}, blobId: {}",
            blobId.getGeneration(), blobId.getBucket(), blobId.getName(), blobId);
  }
}
