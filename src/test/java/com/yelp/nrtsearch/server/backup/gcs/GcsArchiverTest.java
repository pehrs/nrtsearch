/*
 * Copyright 2020 Yelp Inc.
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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.util.IOUtils;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.Tar.CompressionMode;
import com.yelp.nrtsearch.server.backup.TarEntry;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.backup.TarImplTest;
import com.yelp.nrtsearch.server.backup.gcs.GcsStorageApi.GcsBlobId;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsArchiverTest {

  private static final Logger logger = LoggerFactory.getLogger(GcsArchiverTest.class);
  public static final String LZ4_SUFFIX = ".tar.lz4";
  public static final String GZ_SUFFIX = ".tgz";

  private final String BUCKET_NAME = "gcs-archiver-unittest";
  private final String TEST_FILE = "foo";
  private final String TEST_CONTENT = "testcontent";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private Archiver lz4Archiver;
  private Archiver gzArchiver;
  private GcsStorageLocalFs lz4Storage;
  private GcsStorageLocalFs gzStorage;
  private Path lz4ArchiverDirectory;
  private Path gzArchiverDirectory;

  @Before
  public void setup() throws IOException {
    folder.create();

    this.lz4Storage =
        new GcsStorageLocalFs(folder.newFolder("gcs-lz4-simulator").toPath(), LZ4_SUFFIX);
    this.lz4ArchiverDirectory = folder.newFolder("gcs-lz4").toPath();
    TarImpl tar = new TarImpl(CompressionMode.LZ4);
    lz4Archiver = new GcsArchiver(lz4Storage, BUCKET_NAME, lz4ArchiverDirectory, tar, false);

    this.gzStorage =
        new GcsStorageLocalFs(folder.newFolder("gcs-gz-simulator").toPath(), GZ_SUFFIX);
    this.gzArchiverDirectory = folder.newFolder("gcs-gz").toPath();
    TarImpl gzTar = new TarImpl(CompressionMode.GZIP);
    gzArchiver = new GcsArchiver(gzStorage, BUCKET_NAME, gzArchiverDirectory, gzTar, false);
  }

  /**
   * Generate data:
   *
   * <pre>
   *   tar cvf - -C testresource-v1 . | lz4 -f - testresource-v1.tar.lz4
   *   lz4cat testresource.tar.lz4 | tar tvf -
   * </pre>
   *
   * @throws IOException
   */
  @Test
  public void testLz4Download() throws IOException {

    String serviceName = "testservice";
    String resource = "testresource";

    Long generationId = 424242L;
    mockDownloadFromResources(lz4Storage, serviceName, resource, generationId, LZ4_SUFFIX);
    testDownload(lz4Archiver, serviceName, resource, TEST_FILE, TEST_CONTENT);
  }

  @Test
  public void testGzDownload() throws IOException {

    String serviceName = "testservice";
    String resource = "testresource";

    Long generationId = 424242L;
    mockDownloadFromResources(gzStorage, serviceName, resource, generationId, GZ_SUFFIX);
    testDownload(gzArchiver, serviceName, resource, TEST_FILE, TEST_CONTENT);
  }

  @Test
  public void testLz4Upload() throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(lz4ArchiverDirectory, service, resource);
    String subDirPath = sourceDir.resolve("subDir").toString();

    testLz4UploadWithParameters(service, resource, sourceDir, List.of(), List.of(), List.of());
    testLz4UploadWithParameters(
        service, resource, sourceDir, List.of("test1"), List.of(), List.of("test2", "subDir"));
    testLz4UploadWithParameters(
        service, resource, sourceDir, List.of(), List.of(subDirPath), List.of("test1"));
    testLz4UploadWithParameters(
        service, resource, sourceDir, List.of("test1"), List.of(subDirPath), List.of());
  }

  @Test
  public void testGzUpload() throws IOException {
    String service = "testservice";
    String resource = "testresource";
    Path sourceDir = createDirWithFiles(gzArchiverDirectory, service, resource);
    String subDirPath = sourceDir.resolve("subDir").toString();

    testGzUploadWithParameters(service, resource, sourceDir, List.of(), List.of(), List.of());
    testGzUploadWithParameters(
        service, resource, sourceDir, List.of("test1"), List.of(), List.of("test2", "subDir"));
    testGzUploadWithParameters(
        service, resource, sourceDir, List.of(), List.of(subDirPath), List.of("test1"));
    testGzUploadWithParameters(
        service, resource, sourceDir, List.of("test1"), List.of(subDirPath), List.of());
  }

  @Test
  public void testUploadDownload() throws IOException {
    // FIXME: Do we need to test compression both variants here?
    String service = "testservice";
    String resource = "testresource";
    String suffix = LZ4_SUFFIX;
    Archiver archiver = lz4Archiver;
    Path archiverDirectory = lz4ArchiverDirectory;
    Path sourceDir = createDirWithFiles(archiverDirectory, service, resource);

    String versionHash = archiver.upload(service, resource, sourceDir, List.of(), List.of(), false);
    archiver.blessVersion(service, resource, versionHash);
    Path downloadPath = archiver.download(service, resource);
    Path parentPath = downloadPath.getParent();
    Path path = parentPath.resolve(versionHash);
    assertTrue(path.toFile().exists());
  }

  @Test
  public void testCleanup() throws IOException {
    String suffix = LZ4_SUFFIX;
    Archiver archiver = lz4Archiver;
    Path archiverDirectory = lz4ArchiverDirectory;
    GcsStorageLocalFs storage = lz4Storage;

    final Path dontDeleteThisDirectory =
        Files.createDirectory(archiverDirectory.resolve("somerandomsubdirectory"));

    final TarEntry tarEntry = new TarEntry("testresource/foo", "testcontent");
    byte[] tar1Bytes = TarEntry.getTarFile(Arrays.asList(tarEntry));
    Long tar1genId = 12345678942L;
    storage.create(
        GcsBlobId.of(BUCKET_NAME, "testservice/testresource.tar.lz4"), tar1Bytes, tar1genId);

    byte[] tar2Bytes = TarEntry.getTarFile(Arrays.asList(tarEntry));
    Long tar2genId = 12345678943L;
    storage.create(
        GcsBlobId.of(BUCKET_NAME, "testservice/testresource.tar.lz4"), tar2Bytes, tar2genId);

    final Path firstLocation = archiver.download("testservice", "testresource").toRealPath();
    Assert.assertTrue(Files.exists(firstLocation.resolve("testresource/foo")));

    //    s3.putObject(BUCKET_NAME, "testservice/_version/testresource/1", "cafe");
    //
    //    final Path secondLocation = archiver.download("testservice", "testresource").toRealPath();
    //
    //    Assert.assertFalse(Files.exists(firstLocation.resolve("testresource/foo")));
    //    Assert.assertTrue(Files.exists(secondLocation.resolve("testresource/foo")));
    //    Assert.assertTrue(Files.exists(dontDeleteThisDirectory));
  }

  @Test
  public void testGetResources() throws IOException {
    // FIXME: Do we need to test compression both variants here?
    String service = "testservice";
    String[] resources = new String[] {"testresource"};
    String suffix = LZ4_SUFFIX;
    Archiver archiver = lz4Archiver;
    Path archiverDirectory = lz4ArchiverDirectory;

    Path sourceDir = createDirWithFiles(archiverDirectory, service, resources[0]);
    String versionHash =
        archiver.upload(service, resources[0], sourceDir, List.of(), List.of(), false);
    archiver.blessVersion(service, resources[0], versionHash);
    List<String> actualResources = archiver.getResources(service);
    String[] actual = actualResources.toArray(new String[0]);
    Assert.assertArrayEquals(resources, actual);
  }

  private void mockDownloadFromResources(
      GcsStorageLocalFs storage,
      String serviceName,
      String resource,
      Long generationId,
      String suffix)
      throws IOException {
    final ClassLoader classLoader = getClass().getClassLoader();
    String gsPath = String.format("%s/%s%s", serviceName, resource, suffix);
    final GcsBlobId gsPathId = GcsBlobId.of(BUCKET_NAME, gsPath);
    //    Blob gsPathBlob = Mockito.mock(Blob.class);
    //    when(gsPathBlob.getGeneration()).thenReturn(generationId);
    //    when(storage.get(gsPathId,
    // BlobGetOption.fields(BlobField.GENERATION))).thenReturn(gsPathBlob);
    String resourcePath = String.format("gcs/%s", gsPath);

    byte[] fileData = Files.readAllBytes(Path.of(classLoader.getResource(resourcePath).getFile()));
    storage.create(gsPathId, fileData, generationId);
  }

  private void testLz4UploadWithParameters(
      String service,
      String resource,
      Path sourceDir,
      List<String> includeFiles,
      List<String> includeDirs,
      List<String> ignoreVerifying)
      throws IOException {
    testUploadWithParameters(
        CompressionMode.LZ4,
        service,
        resource,
        sourceDir,
        includeFiles,
        includeDirs,
        ignoreVerifying);
  }

  private void testGzUploadWithParameters(
      String service,
      String resource,
      Path sourceDir,
      List<String> includeFiles,
      List<String> includeDirs,
      List<String> ignoreVerifying)
      throws IOException {
    testUploadWithParameters(
        CompressionMode.GZIP,
        service,
        resource,
        sourceDir,
        includeFiles,
        includeDirs,
        ignoreVerifying);
  }

  private void testUploadWithParameters(
      CompressionMode compressionMode,
      String service,
      String resource,
      Path sourceDir,
      List<String> includeFiles,
      List<String> includeDirs,
      List<String> ignoreVerifying)
      throws IOException {

    Archiver archiver = this.lz4Archiver;
    Path archiverDirectory = lz4ArchiverDirectory;
    String suffix = LZ4_SUFFIX;
    GcsStorageLocalFs storage = this.lz4Storage;
    if (compressionMode == CompressionMode.GZIP) {
      archiver = this.gzArchiver;
      archiverDirectory = this.gzArchiverDirectory;
      suffix = GZ_SUFFIX;
      storage = gzStorage;
    }

    // storage.get(gsPathId, BlobGetOption.fields(BlobField.GENERATION));

    Long generationId = 424242L;

    // BlobId{bucket=gcs-archiver-unittest, name=testservice/testresource.tar.lz4, generation=null}
    // storage.get(gsPathId, BlobGetOption.fields(BlobField.GENERATION))
    String gsPath = String.format("%s/%s", service, resource);
    GcsBlobId gsPathId = GcsBlobId.of(BUCKET_NAME, gsPath);
    // Blob gsBlob = Mockito.mock(Blob.class);
    // when(gsBlob.getGeneration()).thenReturn(generationId);
    // when(storage.get(gsPathId, BlobGetOption.fields(BlobField.GENERATION))).thenReturn(gsBlob);

    // byte[] fileData = "".getBytes(StandardCharsets.UTF_8);
    // this.lz4Storage.create(gsPathId, fileData, generationId);

    // FIXME: Streaming is decided on the size of the input...
    String versionHash =
        archiver.upload(service, resource, sourceDir, includeFiles, includeDirs, false);
    Long actualGenerationId = Long.parseLong(versionHash);

    // Get the uploaded dir
    //    Blob gsPathBlob = Mockito.mock(Blob.class);
    //    when(gsPathBlob.getGeneration()).thenReturn(generationId);
    Path actualDownloadDir = Files.createDirectory(archiverDirectory.resolve("actualDownload"));

    // /tmp/junit13289663446052377999/gcs-simulator/gcs-archiver-unittest/testservice/testresource.tar.lz4
    // /tmp/junit13289663446052377999/gcs-simulator/gcs-archiver-unittest/testservice/testresource/4242443.tar.lz4
    Path uploadedBytesPath = storage.getAbsolutePath(gsPathId, actualGenerationId);

    // Expand the uploaded file
    if (compressionMode == CompressionMode.LZ4) {
      try (InputStream gcsInputStream = new FileInputStream(uploadedBytesPath.toFile());
          LZ4FrameInputStream lz4CompressorInputStream = new LZ4FrameInputStream(gcsInputStream);
          TarArchiveInputStream tarArchiveInputStream =
              new TarArchiveInputStream(lz4CompressorInputStream)) {
        new TarImpl(TarImpl.CompressionMode.LZ4)
            .extractTar(tarArchiveInputStream, actualDownloadDir);
      }
    } else {
      try (InputStream gcsInputStream = new FileInputStream(uploadedBytesPath.toFile());
          GZIPInputStream gzipInputStream = new GZIPInputStream(gcsInputStream);
          TarArchiveInputStream tarArchiveInputStream =
              new TarArchiveInputStream(gzipInputStream)) {
        new TarImpl(CompressionMode.GZIP).extractTar(tarArchiveInputStream, actualDownloadDir);
      }
    }
    // Verify that it contains the same as the source
    assertTrue(
        TarImplTest.dirsMatch(
            actualDownloadDir.resolve(resource).toFile(), sourceDir.toFile(), ignoreVerifying));

    Files.delete(uploadedBytesPath);
    rmDir(actualDownloadDir);
  }

  //
  // Non GCS Specific test code
  //

  private void testDownload(
      Archiver archiver,
      String serviceName,
      String resource,
      String filename,
      String matchingContent)
      throws IOException {
    final Path location = archiver.download(serviceName, resource);
    final List<String> allLines = Files.readAllLines(location.resolve(filename));

    assertEquals(1, allLines.size());
    assertEquals(matchingContent, allLines.get(0));
  }

  private Path createDirWithFiles(Path archiverDirectory, String service, String resource)
      throws IOException {
    Path serviceDir = Files.createDirectory(archiverDirectory.resolve(service));
    Path resourceDir = Files.createDirectory(serviceDir.resolve(resource));
    Path subDir = Files.createDirectory(resourceDir.resolve("subDir"));
    try (ByteArrayInputStream test1content = new ByteArrayInputStream("test1content".getBytes());
        ByteArrayInputStream test2content = new ByteArrayInputStream("test2content".getBytes());
        FileOutputStream fileOutputStream1 =
            new FileOutputStream(resourceDir.resolve("test1").toFile());
        FileOutputStream fileOutputStream2 =
            new FileOutputStream(subDir.resolve("test2").toFile()); ) {
      IOUtils.copy(test1content, fileOutputStream1);
      IOUtils.copy(test2content, fileOutputStream2);
    }
    return resourceDir;
  }
}
