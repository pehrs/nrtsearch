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
package com.yelp.nrtsearch;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LuceneServer.LuceneServerCommand;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.Tar;
import com.yelp.nrtsearch.server.utils.gcs.GcsArchiver;
import com.yelp.nrtsearch.server.utils.gcs.GcsStorageImpl;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GCSLuceneServerModule extends LuceneServerModule {

  public GCSLuceneServerModule(LuceneServerCommand args) {
    super(args);
  }

  @Inject
  @Singleton
  @Provides
  protected Storage providesStorage(LuceneServerConfiguration luceneServerConfiguration) {

    // FIXME: Support more detailed setup using a SA json for auth
    // Potential future code:
    //    return StorageOptions.newBuilder()
    //        .setCredentials(ServiceAccountCredentials.fromStream(
    //            new FileInputStream("/path/to/my/key.json"))
    //         )
    //        .build()
    //        .getService();

    // FOR Now please do something like:
    // export GOOGLE_APPLICATION_CREDENTIALS=/path/to/my/key.json

    // Just use the default service for now
    return StorageOptions.getDefaultInstance().getService();
  }


  @Inject
  @Singleton
  @Provides
  protected Archiver providesArchiver(
      LuceneServerConfiguration luceneServerConfiguration, Storage storage, Tar tar) {
    Path archiveDir = Paths.get(luceneServerConfiguration.getArchiveDirectory());

    System.out.println("ARCHIVE: " + archiveDir);

    String gcsPrefix = "nrtsearch";
    return new GcsArchiver(
        new GcsStorageImpl(storage),
        luceneServerConfiguration.getBucketName(),
        gcsPrefix,
        archiveDir,
        tar,
        luceneServerConfiguration.getDownloadAsStream());
  }
}
