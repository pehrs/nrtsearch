/*
 * Copyright 2021 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.utils.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Wrapper for the needed GCS Apis */
public class GcsStorageImpl implements GcsStorageApi {

  private final Storage storage;

  public GcsStorageImpl(Storage storage) {
    this.storage = storage;
  }

  @Override
  public GcsBlob get(GcsBlobId blobId) {
    Blob blob = storage.get(blobId.asBlobId());
    return new GcsStorageBlob(blobId, blob.getGeneration());
  }

  @Override
  public void create(GcsBlobId blobId, byte[] bytes) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId.asBlobId()).build();
    storage.create(blobInfo, bytes);
  }

  @Override
  public WritableByteChannel writer(GcsBlobId blobId) {
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId.asBlobId()).build();
    WriteChannel writeChannel = storage.writer(blobInfo);
    return writeChannel;
  }

  @Override
  public InputStream getInputStream(GcsBlobId blobId) {
    ReadChannel reader = storage.reader(blobId.asBlobId());
    return Channels.newInputStream(reader);
  }

  @Override
  public List<String> listArchives(String bucket, String serviceName) {
    throw new RuntimeException("Not implemented yet!!!");
  }

  public class GcsStorageBlob extends GcsBlob {
    public GcsStorageBlob(GcsBlobId id, Long generation) {
      super(id, generation);
    }

    @Override
    public InputStream asInputStream() {
      return getInputStream(this.getId());
    }

    @Override
    public void downloadTo(Path toPath) throws IOException {
      Files.copy(asInputStream(), toPath);
    }
  }
}
