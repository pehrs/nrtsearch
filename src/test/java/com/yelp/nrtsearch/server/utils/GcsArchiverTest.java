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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.yelp.nrtsearch.server.utils.Tar.CompressionMode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class GcsArchiverTest {

  private final String BUCKET_NAME = "gcs-archiver-unittest";

  private Archiver archiver;

  private Storage storage;
  private Path gcsDir;

  @Before
  public void setup() throws IOException {

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    gcsDir = folder.newFolder("gcs").toPath();

    storage = Mockito.mock(Storage.class);
    Path archiverDirectory = folder.newFolder("archiver").toPath();
    TarImpl tar = new TarImpl(CompressionMode.LZ4);
    archiver = new GcsArchiver(storage, BUCKET_NAME, archiverDirectory, tar, false);
  }

  @Test
  public void testGcsApi() throws IOException {

    String projectId = "fake-project-for-unit-testing";
    String bucketName = "bucket";
    String objectName = "my/path/to/object";
    String resourceName = "addDocs.txt";

    Storage storage = Mockito.mock(Storage.class);
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    BlobTargetOption targetOptions = BlobTargetOption.userProject(projectId);
    Blob blob = Mockito.mock(Blob.class);
    when(storage.create(blobInfo, targetOptions)).thenReturn(blob);

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());

    storage.create(blobInfo, Files.readAllBytes(file.toPath()));

    System.out.println(
        "File " + resourceName + " uploaded to bucket " + bucketName + " as " + objectName);
  }

  /**
   * Generate data:
   *
   * <pre>
   *   tar cvf - -C testresource-v1 . | lz4 -f - testresource-v1.tar.lz4
   * </pre>
   *
   * @throws IOException
   */
  @Test
  public void testDownload() throws IOException {

    final ClassLoader classLoader = getClass().getClassLoader();

    String serviceName = "testservice";
    String resource = "testresource";
    BlobId latestVersionBlobId =
        BlobId.of(
            BUCKET_NAME,
            String.format("%s/_version/%s/_latest_version", serviceName, resource),
            null);
    returnResourceForBlobId(latestVersionBlobId);

    BlobId v1BlobId =
        BlobId.of(BUCKET_NAME, String.format("%s/_version/%s/v1", serviceName, resource), null);
    returnResourceForBlobId(v1BlobId);

    String fooBlobName = String.format("%s/%s/testresource-v1.tar.lz4", serviceName, resource);
    BlobId fooBlobId = BlobId.of(BUCKET_NAME, fooBlobName, null);
    Blob fooBlob = Mockito.mock(Blob.class);

    doAnswer(
            invocation -> {
              Object pathArg = invocation.getArgument(0);
              Path targetPath = (Path) pathArg;
              System.out.println(String.format("Download to %s", targetPath));
              Files.createDirectories(targetPath.getParent());

              String resourcePath = String.format("gcs/%s", fooBlobName);
              System.out.println(String.format("resource %s", resourcePath));

              Path resourceFile =
                  new File(classLoader.getResource(resourcePath).getFile()).toPath();

              Files.copy(resourceFile, targetPath);

              return null;
            })
        .when(fooBlob)
        .downloadTo(Mockito.any(Path.class));

    when(storage.get(fooBlobId)).thenReturn(fooBlob);

    final Path location = archiver.download(serviceName, resource);
    final List<String> allLines = Files.readAllLines(location.resolve("foo"));

    assertEquals(1, allLines.size());
    assertEquals("testcontent", allLines.get(0));
  }

  private void returnResourceForBlobId(BlobId blobId) throws FileNotFoundException {

    String resourcePath = String.format("gcs/%s", blobId.getName());
    ClassLoader classLoader = getClass().getClassLoader();
    File resourceFile = new File(classLoader.getResource(resourcePath).getFile());
    when(storage.reader(blobId)).thenReturn(new MockFileReadChannel(resourceFile));
  }
}
