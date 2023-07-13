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
package com.yelp.nrtsearch.server.backup.gcs;

import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.List;

/** A simple, but testable, abstraction for the GCS Storage Api needed for nrtsearch */
public interface GcsStorageApi {

  // Get a blob!
  GcsBlob get(GcsBlobId gsPathId) throws IOException;

  // Upload bytes...
  void create(GcsBlobId blobInfo, byte[] bytes) throws IOException;

  // Upload for larger files
  WritableByteChannel writer(GcsBlobId blobInfo) throws IOException;

  InputStream getInputStream(GcsBlobId blobId) throws IOException;

  List<String> listArchives(String bucket, String serviceName);

  class GcsBlobId {
    private final String bucket;
    private final String path;

    private GcsBlobId(String bucket, String path) {
      this.bucket = bucket;
      this.path = path;
    }

    public static GcsBlobId of(String bucket, String path) {
      return new GcsBlobId(bucket, path);
    }

    public String getBucket() {
      return bucket;
    }

    public String getPath() {
      return path;
    }

    public BlobId asBlobId() {
      return BlobId.of(this.bucket, this.path);
    }

    public Path asRelativePath() {
      // return BlobId.of(this.bucket, this.path);
      return Path.of(bucket, path);
    }
  }

  abstract class GcsBlob {
    private final GcsBlobId id;
    private final Long generation;

    protected GcsBlob(GcsBlobId id, Long generation) {
      this.id = id;
      this.generation = generation;
    }

    public GcsBlobId getId() {
      return id;
    }

    public Long getGeneration() {
      return generation;
    }

    public abstract InputStream asInputStream() throws IOException;

    public abstract void downloadTo(Path toPath) throws IOException;
  }
}
