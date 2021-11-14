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
package com.yelp.nrtsearch.server.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
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

  private final Storage storage;
  private final Path archiverDirectory;
  private final String bucketName;
  private final Tar tar;
  private final boolean downloadAsStream;

  @Inject
  public GcsArchiver(
      final Storage storage,
      final String bucketName,
      final Path archiverDirectory,
      final Tar tar,
      final boolean downloadAsStream) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.archiverDirectory = archiverDirectory;
    this.tar = tar;
    this.downloadAsStream = downloadAsStream;
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    if (!Files.exists(archiverDirectory)) {
      logger.info("GcsArchiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }

    final String latestVersion = getVersionString(serviceName, resource, "_latest_version");
    final String versionHash = getVersionString(serviceName, resource, latestVersion);
    final Path resourceDestDirectory = archiverDirectory.resolve(resource);
    final Path versionDirectory = resourceDestDirectory.resolve(versionHash);
    final Path currentDirectory = resourceDestDirectory.resolve("current");
    final Path tempCurrentLink = resourceDestDirectory.resolve(getTmpName());
    final Path relativeVersionDirectory = Paths.get(versionHash);

    logger.info(
        "Downloading resource {} for service {} version {} to directory {}",
        resource,
        serviceName,
        versionHash,
        versionDirectory);
    getVersionContent(serviceName, resource, versionHash, versionDirectory);

    try {
      logger.info("Point current version symlink to new resource {}", resource);
      Files.createSymbolicLink(tempCurrentLink, relativeVersionDirectory);
      Files.move(tempCurrentLink, currentDirectory, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      if (Files.exists(tempCurrentLink)) {
        FileUtils.deleteDirectory(tempCurrentLink.toFile());
      }
    }
    cleanupFiles(versionHash, resourceDestDirectory);
    return currentDirectory;
  }

  @Override
  public String upload(
      String serviceName,
      String resource,
      Path path,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException {
    return null;
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return false;
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return false;
  }

  @Override
  public List<String> getResources(String serviceName) {
    return null;
  }

  @Override
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    return null;
  }

  private String getTmpName() {
    return UUID.randomUUID().toString() + TMP_SUFFIX;
  }

  private String getVersionString(
      final String serviceName, final String resource, final String version) throws IOException {
    final String absoluteResourcePath =
        String.format("%s/_version/%s/%s", serviceName, resource, version);
    BlobId blobId = BlobId.of(this.bucketName, absoluteResourcePath);
    try (InputStream input = Channels.newInputStream(storage.reader(blobId))) {
      return IOUtils.toString(input);
    }
  }

  private void getVersionContent(
      final String serviceName, final String resource, final String hash, final Path destDirectory)
      throws IOException {
    final String absoluteResourcePath = String.format("%s/%s/%s", serviceName, resource, hash);
    final Path parentDirectory = destDirectory.getParent();
    final Path tmpFile = parentDirectory.resolve(getTmpName());
    final BlobId blobId = BlobId.of(this.bucketName, absoluteResourcePath);

    final InputStream gcsInputStream;
    if (downloadAsStream) {
      logger.info("Streaming download...");
      gcsInputStream = Channels.newInputStream(storage.reader(blobId));
    } else {
      logger.info("File download...");
      Blob blob = storage.get(blobId);
      blob.downloadTo(tmpFile);
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
