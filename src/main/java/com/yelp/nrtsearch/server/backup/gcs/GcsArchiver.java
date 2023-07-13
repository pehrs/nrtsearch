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

import com.google.inject.Inject;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.Tar.CompressionMode;
import com.yelp.nrtsearch.server.backup.VersionedResource;
import com.yelp.nrtsearch.server.backup.gcs.GcsStorageApi.GcsBlob;
import com.yelp.nrtsearch.server.backup.gcs.GcsStorageApi.GcsBlobId;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsArchiver implements Archiver {

  private static final Logger logger = LoggerFactory.getLogger(GcsArchiver.class);
  private static final String TMP_SUFFIX = ".tmp";
  private static final String CURRENT_VERSION_NAME = "current";

  private final GcsStorageApi storage;
  private final Path archiverDirectory;
  private final String bucketName;
  private final Tar tar;
  private final boolean downloadAsStream;
  private final String pathPrefix;

  @Inject
  public GcsArchiver(
      final GcsStorageApi storage,
      final String bucketName,
      final String gcsPathPrefix,
      final Path archiverDirectory,
      final Tar tar,
      final boolean downloadAsStream) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.pathPrefix = gcsPathPrefix;
    this.archiverDirectory = archiverDirectory;
    this.tar = tar;
    this.downloadAsStream = downloadAsStream;
  }

  @Inject
  public GcsArchiver(
      final GcsStorageApi storage,
      final String bucketName,
      final Path archiverDirectory,
      final Tar tar,
      final boolean downloadAsStream) {
    this(storage, bucketName, null, archiverDirectory, tar, downloadAsStream);
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    if (!Files.exists(archiverDirectory)) {
      logger.info("GcsArchiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }

    String gsPath = getGsPath(serviceName, resource);

    GcsBlobId gsPathId = GcsBlobId.of(this.bucketName, gsPath);
    // Make sure we get the generation id
    GcsBlob gsPathBlob = storage.get(gsPathId);
    Long generationId = gsPathBlob.getGeneration();

    final String resourceDir = String.format("%s/%s", serviceName, resource);
    final Path resourceDestDirectory = archiverDirectory.resolve(resourceDir);
    final Path versionDirectory = resourceDestDirectory.resolve("" + generationId);

    logger.info(
        "Downloading resource {} for service {} generation {} to directory {}",
        resource,
        serviceName,
        generationId,
        versionDirectory);
    getBlobContent(gsPathBlob, serviceName, resource, versionDirectory);

    final Path currentDirectory = resourceDestDirectory.resolve("current");
    final Path tempCurrentLink = resourceDestDirectory.resolve(getTmpName());

    // final String versionPath = String.format("%s/%s/%s", serviceName, resource, generationId);
    final String versionPath = String.format("%s", generationId);
    final Path relativeVersionDirectory = Paths.get(versionPath);

    try {
      logger.info("Point current version symlink to new resource {}", resource);
      Files.createSymbolicLink(tempCurrentLink, relativeVersionDirectory);
      Files.move(tempCurrentLink, currentDirectory, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      if (Files.exists(tempCurrentLink)) {
        FileUtils.deleteDirectory(tempCurrentLink.toFile());
      }
    }
    cleanupFiles(versionPath, resourceDestDirectory);
    return currentDirectory;
  }

  @Override
  public String upload(
      String serviceName,
      String resource,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException {
    // FIXME: We ignore the streaming parameter as we decide wether
    //  to use streaming based on the compressed file size.

    String gsPath = getGsPath(serviceName, resource);
    GcsBlobId gsPathId = GcsBlobId.of(this.bucketName, gsPath);
    // BlobInfo blobInfo = BlobInfo.newBuilder(gsPathId).build();

    String suffix = getSuffix();

    // Build the temporary compressed file and upload it to GCS.
    Path tmpFile = Files.createTempFile(String.format("%s-%s", serviceName, resource), suffix);
    try (FileOutputStream compressedStream = new FileOutputStream(tmpFile.toFile())) {
      tar.buildTar(sourceDir, compressedStream, filesToInclude, parentDirectoriesToInclude);

      uploadToGcs(storage, tmpFile.toFile(), gsPathId);
    } catch (Exception ex) {
      throw new IOException(ex);
    }

    // Delete the temporary compressed file.
    Files.delete(tmpFile);

    // Blob gsBlob = storage.get(gsPathId, BlobGetOption.fields(BlobField.GENERATION));
    GcsBlob gsBlob = storage.get(gsPathId);

    return "" + gsBlob.getGeneration();
  }

  private String getSuffix() {
    if (this.tar.getCompressionMode() == CompressionMode.GZIP) {
      return ".tgz";
    } else {
      return ".tar.lz4";
    }
  }

  private static void uploadToGcs(GcsStorageApi storage, File uploadFrom, GcsBlobId blobInfo)
      throws IOException {
    // For small files:
    if (uploadFrom.length() < 1_000_000) {
      byte[] bytes = Files.readAllBytes(uploadFrom.toPath());
      storage.create(blobInfo, bytes);
      return;
    }

    // For big files:
    // When content is not available or large (1MB or more) it is recommended to write it in chunks
    // via the blob's channel writer.
    try (WritableByteChannel writer = storage.writer(blobInfo)) {
      byte[] buffer = new byte[10_240];
      try (InputStream input = Files.newInputStream(uploadFrom.toPath())) {
        int limit;
        while ((limit = input.read(buffer)) >= 0) {
          writer.write(ByteBuffer.wrap(buffer, 0, limit));
        }
      }
    }
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    // FIXME:
    return false;
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    // FIXME:
    return false;
  }

  @Override
  public boolean deleteLocalFiles(String resource) {
    // FIXME:
    return true;
  }

  @Override
  public List<String> getResources(String serviceName) {
    List<String> archives = storage.listArchives(this.bucketName, serviceName);
    if (tar.getCompressionMode() == CompressionMode.LZ4) {
      return archives.stream()
          .map(name -> name.replace(".tar.lz4", ""))
          .collect(Collectors.toList());
    } else {
      return archives.stream().map(name -> name.replace(".tgz", "")).collect(Collectors.toList());
    }
  }

  @Override
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    // FIXME:
    throw new RuntimeException("Not implemented yet!");
  }

  private String getGsPath(String serviceName, String resource) {

    String suffix = "tgz";
    if (tar.getCompressionMode().equals(Tar.CompressionMode.LZ4)) {
      suffix = "tar.lz4";
    }
    if (this.pathPrefix != null && this.pathPrefix.length() > 0) {
      return String.format("%s/%s/%s.%s", this.pathPrefix, serviceName, resource, suffix);
    } else {
      return String.format("%s/%s.%s", serviceName, resource, suffix);
    }
  }

  private String getTmpName() {
    return UUID.randomUUID().toString() + TMP_SUFFIX;
  }

  private String getVersionString(
      final String serviceName, final String resource, final String version) throws IOException {
    final String absoluteResourcePath =
        String.format("%s/_version/%s/%s", serviceName, resource, version);
    GcsBlobId blobId = GcsBlobId.of(this.bucketName, absoluteResourcePath);
    // try (InputStream input = Channels.newInputStream(storage.reader(blobId))) {
    try (InputStream input = storage.getInputStream(blobId)) {
      return IOUtils.toString(input);
    }
  }

  private void getBlobContent(
      final GcsBlob gsPathBlob,
      final String serviceName,
      final String resource,
      final Path destDirectory)
      throws IOException {
    // final String absoluteResourcePath = String.format("%s/%s", serviceName, resource);
    final Path parentDirectory = destDirectory.getParent();
    final Path tmpFile = parentDirectory.resolve(getTmpName());

    final InputStream gcsInputStream;
    if (downloadAsStream) {
      logger.info("Streaming download...");
      // gcsInputStream = Channels.newInputStream(gsPathBlob.reader());
      gcsInputStream = gsPathBlob.asInputStream();
    } else {
      logger.info("Blob download...");
      gsPathBlob.downloadTo(tmpFile);
      gcsInputStream = new FileInputStream(tmpFile.toFile());
    }
    logger.info("Object streaming started...");

    final InputStream compressorInputStream;
    if (tar.getCompressionMode().equals(Tar.CompressionMode.LZ4)) {
      compressorInputStream = new LZ4FrameInputStream(gcsInputStream);
    } else {
      compressorInputStream = new GzipCompressorInputStream(gcsInputStream, true);
    }
    try (final TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(compressorInputStream); ) {
      if (Files.exists(destDirectory)) {
        logger.info("Directory {} already exists, not re-downloading from Archiver", destDirectory);
        return;
      }
      final Path tmpDirectory = parentDirectory.resolve(getTmpName());
      try {
        tar.extractTar(tarArchiveInputStream, tmpDirectory);
        Files.move(tmpDirectory, destDirectory);
      } catch (Exception ex) {
        ex.printStackTrace();
        throw new RuntimeException(ex);
      } finally {
        if (Files.exists(tmpDirectory)) {
          FileUtils.deleteDirectory(tmpDirectory.toFile());
        }
        if (Files.exists(tmpFile)) {
          Files.delete(tmpFile);
        }
      }
    }
  }

  private void cleanupFiles(final String versionHash, final Path resourceDestDirectory)
      throws IOException {
    final DirectoryStream.Filter<Path> filter =
        entry -> {
          final String fileName = entry.getFileName().toString();
          // Ignore the current version
          if (CURRENT_VERSION_NAME.equals(fileName)) {
            return false;
          }
          // Ignore the current version hash
          if (versionHash.equals(fileName)) {
            return false;
          }
          // Ignore non-directories
          if (!Files.isDirectory(entry)) {
            logger.warn("Unexpected non-directory entry found while cleaning up: {}", fileName);
            return false;
          }
          // Ignore version names that aren't hex encoded
          try {
            Hex.decodeHex(fileName.toCharArray());
          } catch (DecoderException e) {
            logger.warn(
                "Not cleaning up directory because name doesn't match pattern: {}", fileName);
            return false;
          }
          return true;
        };
    try (final DirectoryStream<Path> stream =
        Files.newDirectoryStream(resourceDestDirectory, filter)) {
      for (final Path entry : stream) {
        logger.info("Cleaning up old directory: {}", entry);
        FileUtils.deleteDirectory(entry.toFile());
      }
    }
  }
}
