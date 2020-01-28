/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.fs;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metrics.Metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * HoodieWrapperFileSystem wraps the default file system. It holds state about the open streams in the file system to
 * support getting the written size to each of the open streams.
 */
public class HoodieWrapperFileSystem extends FileSystem {

  public static final String HOODIE_SCHEME_PREFIX = "hoodie-";

  private ConcurrentMap<String, SizeAwareFSDataOutputStream> openStreams = new ConcurrentHashMap<>();
  private FileSystem fileSystem;
  private URI uri;
  private ConsistencyGuard consistencyGuard = new NoOpConsistencyGuard();
  private Metrics metrics;
  private String metricPrefix = "fs.";
  private ConcurrentHashMap<String, Timer> timerMap = new ConcurrentHashMap<String, Timer>();

  public HoodieWrapperFileSystem() {}

  public HoodieWrapperFileSystem(FileSystem fileSystem, ConsistencyGuard consistencyGuard) {
    this.fileSystem = fileSystem;
    this.uri = fileSystem.getUri();
    this.consistencyGuard = consistencyGuard;
    this.metrics = Metrics.isInitialized() ? Metrics.getInstance() : null;
  }

  public static Path convertToHoodiePath(Path file, Configuration conf) {
    try {
      String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
      return convertPathWithScheme(file, getHoodieScheme(scheme));
    } catch (HoodieIOException e) {
      throw e;
    }
  }

  private static Path convertPathWithScheme(Path oldPath, String newScheme) {
    URI oldURI = oldPath.toUri();
    URI newURI;
    try {
      newURI = new URI(newScheme, oldURI.getUserInfo(), oldURI.getHost(), oldURI.getPort(), oldURI.getPath(),
          oldURI.getQuery(), oldURI.getFragment());
      return new Path(newURI);
    } catch (URISyntaxException e) {
      // TODO - Better Exception handling
      throw new RuntimeException(e);
    }
  }

  public static String getHoodieScheme(String scheme) {
    String newScheme;
    if (StorageSchemes.isSchemeSupported(scheme)) {
      newScheme = HOODIE_SCHEME_PREFIX + scheme;
    } else {
      throw new IllegalArgumentException("BlockAlignedAvroParquetWriter does not support scheme " + scheme);
    }
    return newScheme;
  }

  @Override
  public void initialize(URI uri, Configuration conf) {
    // Get the default filesystem to decorate
    Path path = new Path(uri);
    // Remove 'hoodie-' prefix from path
    if (path.toString().startsWith(HOODIE_SCHEME_PREFIX)) {
      path = new Path(path.toString().replace(HOODIE_SCHEME_PREFIX, ""));
      this.uri = path.toUri();
    } else {
      this.uri = uri;
    }
    this.fileSystem = FSUtils.getFs(path.toString(), conf);
    // Do not need to explicitly initialize the default filesystem, its done already in the above
    // FileSystem.get
    // fileSystem.initialize(FileSystem.getDefaultUri(conf), conf);
    // fileSystem.setConf(conf);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Timer.Context timerCtx = getTimerContext("open");
    try {
      return fileSystem.open(convertToDefaultPath(f), bufferSize);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      final Path translatedPath = convertToDefaultPath(f);
      return wrapOutputStream(f,
          fileSystem.create(translatedPath, permission, overwrite, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  private FSDataOutputStream wrapOutputStream(final Path path, FSDataOutputStream fsDataOutputStream)
      throws IOException {
    if (fsDataOutputStream instanceof SizeAwareFSDataOutputStream) {
      return fsDataOutputStream;
    }

    SizeAwareFSDataOutputStream os = new SizeAwareFSDataOutputStream(path, fsDataOutputStream, consistencyGuard,
        () -> openStreams.remove(path.getName()));
    openStreams.put(path.getName(), os);
    return os;
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f)));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), replication));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), replication, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), permission, flags, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f, fileSystem.create(convertToDefaultPath(f), permission, flags, bufferSize, replication,
          blockSize, progress, checksumOpt));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return wrapOutputStream(f,
          fileSystem.create(convertToDefaultPath(f), overwrite, bufferSize, replication, blockSize));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("append");
    try {
      return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f), bufferSize, progress));
    } finally {
      updateMetrics(timerCtx, "write", bufferSize);
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("rename");
    try {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(src));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for " + src + " to appear", e);
      }

      boolean success = fileSystem.rename(convertToDefaultPath(src), convertToDefaultPath(dst));

      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + dst + " to appear", e);
        }

        try {
          consistencyGuard.waitTillFileDisappears(convertToDefaultPath(src));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + src + " to disappear", e);
        }
      }
      return success;
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Timer.Context timerCtx = getTimerContext("delete");
    try {
      boolean success = fileSystem.delete(convertToDefaultPath(f), recursive);

      if (success) {
        try {
          consistencyGuard.waitTillFileDisappears(f);
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + f + " to disappear", e);
        }
      }
      return success;
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listStatus(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return convertToHoodiePath(fileSystem.getWorkingDirectory());
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    fileSystem.setWorkingDirectory(convertToDefaultPath(newDir));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Timer.Context timerCtx = getTimerContext("mkdir");
    try {
      boolean success = fileSystem.mkdirs(convertToDefaultPath(f), permission);
      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for directory " + f + " to appear", e);
        }
      }
      return success;
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
      } catch (TimeoutException e) {
        // pass
      }
      return fileSystem.getFileStatus(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public String getCanonicalServiceName() {
    return fileSystem.getCanonicalServiceName();
  }

  @Override
  public String getName() {
    return fileSystem.getName();
  }

  @Override
  public Path makeQualified(Path path) {
    return convertToHoodiePath(fileSystem.makeQualified(convertToDefaultPath(path)));
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getDelegationToken(renewer);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      return fileSystem.addDelegationTokens(renewer, credentials);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    return fileSystem.getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Timer.Context timerCtx = getTimerContext("getblock");
    try {
      return fileSystem.getFileBlockLocations(file, start, len);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    Timer.Context timerCtx = getTimerContext("getblock");
    try {
      return fileSystem.getFileBlockLocations(convertToDefaultPath(p), start, len);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fileSystem.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return fileSystem.getServerDefaults(convertToDefaultPath(p));
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    return convertToHoodiePath(fileSystem.resolvePath(convertToDefaultPath(p)));
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("open");
    try {
      return fileSystem.open(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      Path p = convertToDefaultPath(f);
      return wrapOutputStream(p,
          fileSystem.createNonRecursive(p, overwrite, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      Path p = convertToDefaultPath(f);
      return wrapOutputStream(p,
          fileSystem.createNonRecursive(p, permission, overwrite, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      Path p = convertToDefaultPath(f);
      return wrapOutputStream(p,
          fileSystem.createNonRecursive(p, permission, flags, bufferSize, replication, blockSize, progress));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      boolean newFile = fileSystem.createNewFile(convertToDefaultPath(f));
      if (newFile) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for " + f + " to appear", e);
        }
      }
      return newFile;
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("append");
    try {
      return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f)));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    Timer.Context timerCtx = getTimerContext("append");
    try {
      return wrapOutputStream(f, fileSystem.append(convertToDefaultPath(f), bufferSize));
    } finally {
      updateMetrics(timerCtx, "write", bufferSize);
    }
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    Timer.Context timerCtx = getTimerContext("concat");
    try {
      Path[] psrcsNew = convertDefaults(psrcs);
      fileSystem.concat(convertToDefaultPath(trg), psrcsNew);
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(trg));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for " + trg + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public short getReplication(Path src) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getReplication(convertToDefaultPath(src));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      return fileSystem.setReplication(convertToDefaultPath(src), replication);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean delete(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("delete");
    try {
      return delete(f, true);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      return fileSystem.deleteOnExit(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      return fileSystem.cancelDeleteOnExit(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("exists");
    try {
      return fileSystem.exists(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.isDirectory(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.isFile(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public long getLength(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getLength(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getContentSummary(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("getblock");
    try {
      return fileSystem.listCorruptFileBlocks(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listStatus(convertToDefaultPath(f), filter);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listStatus(convertDefaults(files));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listStatus(convertDefaults(files), filter);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.globStatus(convertToDefaultPath(pathPattern));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.globStatus(convertToDefaultPath(pathPattern), filter);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listLocatedStatus(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    Timer.Context timerCtx = getTimerContext("list");
    try {
      return fileSystem.listFiles(convertToDefaultPath(f), recursive);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Path getHomeDirectory() {
    return convertToHoodiePath(fileSystem.getHomeDirectory());
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("mkdir");
    try {
      boolean success = fileSystem.mkdirs(convertToDefaultPath(f));
      if (success) {
        try {
          consistencyGuard.waitTillFileAppears(convertToDefaultPath(f));
        } catch (TimeoutException e) {
          throw new HoodieException("Timed out waiting for directory " + f + " to appear", e);
        }
      }
      return success;
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("move");
    try {
      fileSystem.moveFromLocalFile(convertLocalPaths(srcs), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("move");
    try {
      fileSystem.moveFromLocalFile(convertToLocalPath(src), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyFromLocalFile(delSrc, convertToLocalPath(src), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyFromLocalFile(delSrc, overwrite, convertLocalPaths(srcs), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyFromLocalFile(delSrc, overwrite, convertToLocalPath(src), convertToDefaultPath(dst));
      try {
        consistencyGuard.waitTillFileAppears(convertToDefaultPath(dst));
      } catch (TimeoutException e) {
        throw new HoodieException("Timed out waiting for destination " + dst + " to appear", e);
      }
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("move");
    try {
      fileSystem.moveToLocalFile(convertToDefaultPath(src), convertToLocalPath(dst));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    Timer.Context timerCtx = getTimerContext("copy");
    try {
      fileSystem.copyToLocalFile(delSrc, convertToDefaultPath(src), convertToLocalPath(dst), useRawLocalFileSystem);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    return convertToHoodiePath(
        fileSystem.startLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile)));
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    fileSystem.completeLocalOutput(convertToDefaultPath(fsOutputFile), convertToDefaultPath(tmpLocalFile));
  }

  @Override
  public void close() throws IOException {
    // Don't close the wrapped `fileSystem` object. This will end up closing it for every thread since it
    // could be cached across jvm. We don't own that object anyway.
    super.close();
  }

  @Override
  public long getUsed() throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getUsed();
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public long getBlockSize(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getBlockSize(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public long getDefaultBlockSize() {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getDefaultBlockSize();
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getDefaultBlockSize(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public short getDefaultReplication() {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getDefaultReplication();
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public short getDefaultReplication(Path path) {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getDefaultReplication(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    Timer.Context timerCtx = getTimerContext("access");
    try {
      fileSystem.access(convertToDefaultPath(path), mode);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      fileSystem.createSymlink(convertToDefaultPath(target), convertToDefaultPath(link), createParent);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getFileLinkStatus(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public boolean supportsSymlinks() {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.supportsSymlinks();
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    return convertToHoodiePath(fileSystem.getLinkTarget(convertToDefaultPath(f)));
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getFileChecksum(convertToDefaultPath(f));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getFileChecksum(convertToDefaultPath(f), length);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setVerifyChecksum(verifyChecksum);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setWriteChecksum(writeChecksum);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FsStatus getStatus() throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getStatus();
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getStatus(convertToDefaultPath(p));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setPermission(convertToDefaultPath(p), permission);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setOwner(convertToDefaultPath(p), username, groupname);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setTimes(convertToDefaultPath(p), mtime, atime);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    Timer.Context timerCtx = getTimerContext("create");
    try {
      return convertToHoodiePath(fileSystem.createSnapshot(convertToDefaultPath(path), snapshotName));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    Timer.Context timerCtx = getTimerContext("rename");
    try {
      fileSystem.renameSnapshot(convertToDefaultPath(path), snapshotOldName, snapshotNewName);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    Timer.Context timerCtx = getTimerContext("delete");
    try {
      fileSystem.deleteSnapshot(convertToDefaultPath(path), snapshotName);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.modifyAclEntries(convertToDefaultPath(path), aclSpec);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.removeAclEntries(convertToDefaultPath(path), aclSpec);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.removeDefaultAcl(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.removeAcl(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setAcl(convertToDefaultPath(path), aclSpec);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getAclStatus(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setXAttr(convertToDefaultPath(path), name, value);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.setXAttr(convertToDefaultPath(path), name, value, flag);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getXAttr(convertToDefaultPath(path), name);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getXAttrs(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.getXAttrs(convertToDefaultPath(path), names);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    Timer.Context timerCtx = getTimerContext("get");
    try {
      return fileSystem.listXAttrs(convertToDefaultPath(path));
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    Timer.Context timerCtx = getTimerContext("set");
    try {
      fileSystem.removeXAttr(convertToDefaultPath(path), name);
    } finally {
      updateMetrics(timerCtx);
    }
  }

  @Override
  public Configuration getConf() {
    return fileSystem.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    // ignore this. we will set conf on init
  }

  @Override
  public int hashCode() {
    return fileSystem.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return fileSystem.equals(obj);
  }

  @Override
  public String toString() {
    return fileSystem.toString();
  }

  public Path convertToHoodiePath(Path oldPath) {
    return convertPathWithScheme(oldPath, getHoodieScheme(getScheme()));
  }

  private Path convertToDefaultPath(Path oldPath) {
    return convertPathWithScheme(oldPath, getScheme());
  }

  private Path convertToLocalPath(Path oldPath) {
    try {
      return convertPathWithScheme(oldPath, FileSystem.getLocal(getConf()).getScheme());
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private Path[] convertLocalPaths(Path[] psrcs) {
    Path[] psrcsNew = new Path[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      psrcsNew[i] = convertToLocalPath(psrcs[i]);
    }
    return psrcsNew;
  }

  private Path[] convertDefaults(Path[] psrcs) {
    Path[] psrcsNew = new Path[psrcs.length];
    for (int i = 0; i < psrcs.length; i++) {
      psrcsNew[i] = convertToDefaultPath(psrcs[i]);
    }
    return psrcsNew;
  }

  public long getBytesWritten(Path file) {
    if (openStreams.containsKey(file.getName())) {
      return openStreams.get(file.getName()).getBytesWritten();
    }
    // When the file is first written, we do not have a track of it
    throw new IllegalArgumentException(
        file.toString() + " does not have a open stream. Cannot get the bytes written on the stream");
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Returns a {@link Timer.Context} which can be used to time an operation.
   *
   * @param operationName The name of the operation which is being timed.
   *                      The timer itself is registered with the name
   *                      <metricPrefix>.<operationName>
   * @return A {@link Timer.Context} or null if metrics are disabled
   */
  private Timer.Context getTimerContext(String operationName) {
    if (metrics != null) {
      Timer timer = timerMap.get(operationName);
      if (timer == null) {
        timer = Metrics.createTimer(metricPrefix + operationName);
        timerMap.putIfAbsent(operationName, timer);
      }

      return timer.time();
    }
    return null;
  }

  /**
   * Update the time spent on an operation.
   *
   * @param timerCtx A {@link Timer.Context} returned by getTimerContext. May be null
   *                 if metrics are disabled.
   */
  private void updateMetrics(Timer.Context timerCtx) {
    if (timerCtx != null) {
      timerCtx.stop();
    }
  }

  /**
   * Update the time spent on an operation and an associated {@link Counter}.
   *
   * @param timerCtx A {@link Timer.Context} returned by getTimerContext. May be null
   *                 if metrics are disabled.
   * @param counterName Name of the Counter to increment
   * @param increment The value to increment the Counter by.
   */
  private void updateMetrics(Timer.Context timerCtx, String counterName,
                             long increment) {
    updateMetrics(timerCtx);

    if (metrics != null) {
      Counter counter = Metrics.createCounter(counterName);
      counter.inc(increment);
    }
  }

}
