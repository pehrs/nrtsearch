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

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class MockFileReadChannel implements ReadChannel {

  private FileChannel wrapped;

  public MockFileReadChannel(FileInputStream fileInput) {
    this.wrapped = fileInput.getChannel();
  }

  public MockFileReadChannel(File file) throws FileNotFoundException {
    this(new FileInputStream(file));
  }

  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return wrapped.read(dsts, offset, length);
  }

  public long read(ByteBuffer[] dsts) throws IOException {
    return wrapped.read(dsts);
  }

  public int write(ByteBuffer src) throws IOException {
    return wrapped.write(src);
  }

  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    return wrapped.write(srcs, offset, length);
  }

  public long write(ByteBuffer[] srcs) throws IOException {
    return wrapped.write(srcs);
  }

  public long position() throws IOException {
    return wrapped.position();
  }

  public FileChannel position(long newPosition) throws IOException {
    return wrapped.position(newPosition);
  }

  public long size() throws IOException {
    return wrapped.size();
  }

  public FileChannel truncate(long size) throws IOException {
    return wrapped.truncate(size);
  }

  public void force(boolean metaData) throws IOException {
    wrapped.force(metaData);
  }

  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return wrapped.transferTo(position, count, target);
  }

  public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
    return wrapped.transferFrom(src, position, count);
  }

  public int read(ByteBuffer dst, long position) throws IOException {
    return wrapped.read(dst, position);
  }

  public int write(ByteBuffer src, long position) throws IOException {
    return wrapped.write(src, position);
  }

  public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
    return wrapped.map(mode, position, size);
  }

  public FileLock lock(long position, long size, boolean shared) throws IOException {
    return wrapped.lock(position, size, shared);
  }

  public FileLock lock() throws IOException {
    return wrapped.lock();
  }

  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return wrapped.tryLock(position, size, shared);
  }

  public FileLock tryLock() throws IOException {
    return wrapped.tryLock();
  }

  @Override
  public boolean isOpen() {
    return wrapped.isOpen();
  }

  @Override
  public void close() {
    try {
      wrapped.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return wrapped.read(dst);
  }

  @Override
  public void seek(long position) throws IOException {
    throw new IllegalStateException("seek() is not supported");
  }

  @Override
  public void setChunkSize(int chunkSize) {
    // Ignore for now :-)
  }

  @Override
  public RestorableState<ReadChannel> capture() {
    throw new IllegalStateException("capture() is not supported");
  }
}
