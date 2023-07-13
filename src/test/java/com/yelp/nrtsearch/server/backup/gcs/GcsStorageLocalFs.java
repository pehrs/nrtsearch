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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** A simple utility to run a faked GCS for testing */
public class GcsStorageLocalFs implements GcsStorageApi {

  private final Path rootDir;
  private final String suffix;

  public GcsStorageLocalFs(Path rootDir, String suffix) {
    this.rootDir = rootDir;
    this.suffix = suffix;
  }

  public Path getRootDir() {
    return rootDir;
  }

  @Override
  public GcsBlob get(GcsBlobId blobId) throws IOException {
    // FIXME: Maybe change the api to use Optional<> ?
    return getLatestGeneration(blobId).map(gen -> new GcsFsBlob(blobId, gen)).orElseThrow();
  }

  public Path getAbsolutePath(GcsBlobId blobId, Long generationId) {
    return new File(
            String.format(
                "%s/%s/%s%s/%d",
                getRootDir(), blobId.getBucket(), blobId.getPath(), this.suffix, generationId))
        .toPath();
  }

  public Optional<Path> getLatestGenerationPath(GcsBlobId blobId) throws IOException {
    // └── gcs-simulator
    //    └── gcs-archiver-unittest
    //        └── testservice
    //            └── resource.tgz
    //                ├── 00000043
    //                └── 00000044
    Path resourcePath = this.rootDir.resolve(blobId.asRelativePath());

    String[] resourceGens = resourcePath.toFile().list();

    if (resourceGens == null) {
      return Optional.empty();
    }
    if (resourceGens.length == 0) {
      return Optional.empty();
    }

    return Arrays.stream(resourceGens)
        .sorted(Comparator.reverseOrder())
        .findFirst()
        .map(str -> Path.of(resourcePath.toString(), str));
  }

  public Optional<Long> getGeneration(Optional<Path> path) {
    return path.map(value -> Long.parseLong(value.toString().replaceAll(".*\\/", "")));
    // .replaceAll("\\..*", "")
  }

  public Optional<Long> getLatestGeneration(GcsBlobId blobId) throws IOException {
    return getGeneration(getLatestGenerationPath(blobId));
  }

  //  private static Path getGenPath(Path absolutePath) {
  //    return new File(String.format("%s.gen", absolutePath.toString())).toPath();
  //  }

  @Override
  public void create(GcsBlobId blobId, byte[] content) throws IOException {
    long generation = System.nanoTime();
    create(blobId, content, generation);
  }

  public void create(GcsBlobId blobId, byte[] content, Long generation) throws IOException {
    Path absolutePath = rootDir.resolve(blobId.asRelativePath());
    if (!absolutePath.toFile().exists()) {
      Files.createDirectories(absolutePath);
    }
    // └── gcs-simulator
    //    └── gcs-archiver-unittest
    //        └── testservice
    //            └── resource.tgz
    //                ├── 00000043
    //                └── 00000044

    Path genPath = new File(String.format("%s/%d", absolutePath.toString(), generation)).toPath();

    Files.write(genPath, content);
  }

  @Override
  public WritableByteChannel writer(GcsBlobId blobId) throws IOException {
    long generation = System.nanoTime();
    Path absolutePath = getAbsolutePath(blobId, generation);
    if (!absolutePath.getParent().toFile().exists()) {
      Files.createDirectories(absolutePath.getParent());
    }
    SeekableByteChannel channel = Files.newByteChannel(absolutePath);

    return channel;
  }

  @Override
  public InputStream getInputStream(GcsBlobId blobId) throws IOException {
    Optional<Path> absolutePath = getLatestGenerationPath(blobId);
    return absolutePath
        .map(
            absPath -> {
              try {
                if (!absPath.getParent().toFile().exists()) {
                  Files.createDirectories(absPath.getParent());
                }
                return Files.newInputStream(absPath);
              } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
            })
        .orElseThrow();
  }

  public InputStream getInputStream(GcsBlobId blobId, Long generation) throws IOException {
    Path absolutePath = rootDir.resolve(blobId.asRelativePath());
    if (!absolutePath.toFile().exists()) {
      Files.createDirectories(absolutePath);
    }
    // └── gcs-simulator
    //    └── gcs-archiver-unittest
    //        └── testservice
    //            └── resource.tgz
    //                ├── 00000043
    //                └── 00000044
    Path genPath = new File(String.format("%s/%d", absolutePath.toString(), generation)).toPath();

    return Files.newInputStream(genPath);
  }

  @Override
  public List<String> listArchives(String bucket, String serviceName) {

    /// tmp/junit13941733087764847930
    // └── gcs-simulator
    //    └── gcs-archiver-unittest
    //        └── testservice.tgz
    //            ├── 43434343
    //            └── 43434344

    final Path dir = getRootDir().resolve(bucket).resolve(serviceName);
    if (dir.toFile().exists()) {
      return Arrays.stream(dir.toFile().list()).collect(Collectors.toList());
      //      return Arrays.stream(dir.toFile().list((d, name) -> name.endsWith(".gen")))
      //          .map(name -> name.replace(".gen", ""))
      //          .collect(Collectors.toList());
    } else {
      return List.of();
    }
  }

  public class GcsFsBlob extends GcsBlob {

    public GcsFsBlob(GcsBlobId id, Long generation) {
      super(id, generation);
    }

    @Override
    public InputStream asInputStream() throws IOException {
      return getInputStream(this.getId(), this.getGeneration());
    }

    @Override
    public void downloadTo(Path toPath) throws IOException {
      if (!toPath.getParent().toFile().exists()) {
        Files.createDirectories(toPath.getParent());
      }
      Files.copy(asInputStream(), toPath);
    }
  }
}
