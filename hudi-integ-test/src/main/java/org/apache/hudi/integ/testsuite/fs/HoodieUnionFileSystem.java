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

package org.apache.hudi.integ.testsuite.fs;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HoodieUnionFileSystem is a union of two directories:
 *  - base directory (read only)
 *  - delta directory (read-write)
 *
 *  The two directories may be in different clusters (production / dev).
 *
 *  All writes are written to the delta directory only.
 */
public class HoodieUnionFileSystem extends HoodieWrapperFileSystem {

  public static final String CONF_PREFIX = "hoodie.fs.union.";
  public static final String CONF_BASEURI = CONF_PREFIX + "baseuri";
  public static final String CONF_DELTAURI = CONF_PREFIX + "deltauri";
  public static final String CONF_MAX_FILE_MOD_TIMESTAMP = CONF_PREFIX + "max.file.modification.timestamp";
  public static final String CONF_MAX_DATAFILE_MOD_TIMESTAMP = CONF_PREFIX + "max.datafile.modification.timestamp";

  private static final String DELETE_SUFFIX = "__DELETED__";

  private String baseDirectory;
  private String deltaDirectory;
  private String maxFileTimestamp;
  private String maxFileTimestampDatafiles;
  private HoodieWrapperFileSystem baseFs;
  private HoodieWrapperFileSystem deltaFs;

  public HoodieUnionFileSystem() {}

  public HoodieUnionFileSystem(FileSystem fileSystem, ConsistencyGuard consistencyGuard) {
    super(fileSystem, consistencyGuard);
    initialize(null, fileSystem.getConf(), consistencyGuard);
  }

  @Override
  public void initialize(URI uri, Configuration conf) {
    initialize(uri, conf, new NoOpConsistencyGuard());
  }

  private void initialize(URI uri, Configuration conf, ConsistencyGuard consistencyGuard) {
    if (conf.get(CONF_BASEURI) == null) {
      throw new HoodieException("BASE URI IS NULL");
    }
    if (conf.get(CONF_DELTAURI) == null) {
      throw new HoodieException("DELTA URI IS NULL");
    }

    // The paths in config may include scheme and authority
    String baseURI = conf.get(CONF_BASEURI);
    String deltaURI = conf.get(CONF_DELTAURI);
    this.maxFileTimestamp = conf.get(CONF_MAX_FILE_MOD_TIMESTAMP);
    this.maxFileTimestampDatafiles = conf.get(CONF_MAX_DATAFILE_MOD_TIMESTAMP, this.maxFileTimestamp);

    // Ensure URIs do not end with /
    if (baseURI.endsWith("/")) {
      baseURI = baseURI.substring(0, baseURI.length() - 1);
    }
    if (deltaURI.endsWith("/")) {
      deltaURI = deltaURI.substring(0, deltaURI.length() - 1);
    }

    // Allocate file systems to use
    Path basePath = new Path(baseURI);
    Path deltaPath = new Path(deltaURI);
    this.baseFs = new HoodieWrapperFileSystem(FSUtils.getRawFs(basePath, conf), consistencyGuard);
    this.deltaFs = new HoodieWrapperFileSystem(FSUtils.getRawFs(deltaPath, conf), consistencyGuard);

    // The base directories in each filesystem
    this.baseDirectory = basePath.toUri().getPath();
    this.deltaDirectory = deltaPath.toUri().getPath();

    LOG.info(String.format("Using base [fs: %s, uri: %s, dir: %s] and delta [fs: %s, uri: %s, dir: %s] with max file "
        + "timestamp %s and max data file timestamp %s", baseFs, baseURI, baseDirectory, deltaFs, deltaURI,
        deltaDirectory, maxFileTimestamp, maxFileTimestampDatafiles));
  }

  private Path convertToDeltaPath(Path path) {
    if (baseDirectory == null || deltaDirectory == null) {
      throw new HoodieException("Base and Delta directories not found");
    }

    // Path should be within the base directory. There may be a schema or server as prefix which we will
    // neglect.
    String pathStr = path.toString();
    int index = pathStr.indexOf(baseDirectory);
    if (index == -1) {
      LOG.error("Path " + path + " not within the base directory " + baseDirectory);
      return path;
    }

    String newPath = deltaDirectory + pathStr.substring(index + baseDirectory.length());
    LOG.info("Converted " + path + " to " + newPath);
    return new Path(newPath);
  }

  private FileStatus convertToBasePath(FileStatus status) {
    if (baseDirectory == null || deltaDirectory == null) {
      throw new HoodieException("Base and Delta directories not found");
    }

    String pathStr = status.getPath().toString().replace(deltaDirectory, baseDirectory);
    Path convertedPath = new Path(pathStr);
    // the schema and authority of base path may be different
    convertedPath = baseFs.convertToDefaultPath(convertedPath);
    status.setPath(convertedPath);
    return status;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try {
      return deltaFs.open(convertToDeltaPath(f), bufferSize);
    } catch (FileNotFoundException e) {
      return baseFs.open(f, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), overwrite);
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f));
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), progress);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), replication);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), replication, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), overwrite, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    // writes only go to the delta directory
    return deltaFs.create(convertToDeltaPath(f), overwrite, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return deltaFs.create(convertToDeltaPath(f), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    return deltaFs.create(convertToDeltaPath(f), permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
    return deltaFs.create(convertToDeltaPath(f), permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return deltaFs.create(convertToDeltaPath(f), overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (deltaFs.exists(convertToDeltaPath(src))) {
      return deltaFs.rename(convertToDeltaPath(src), convertToDeltaPath(dst));
    }

    // Rename will not be atomic as we delete and create the new file separately
    delete(dst);

    boolean copyStatus = FileUtil.copy(baseFs.getFileSystem(), src, deltaFs.getFileSystem(),
        convertToDeltaPath(dst), false, deltaFs.getFileSystem().getConf());

    delete(src);
    deltaFs.delete(new Path(convertToDeltaPath(dst).toString() + DELETE_SUFFIX), false);

    return copyStatus;
  }

  @Override
  public boolean delete(Path f) throws IOException {
    // TODO: Directory deletion is not handled. Not a usecase within Hudi.

    if (baseFs.exists(f)) {
      // File exists in base directory so needs to be deleted via a marker
      // Create a special file with the extension __DELETED__. This file name is never used in HUDI and hence
      // can be used as a marker to signify file deletion.
      Path markerPath = convertToDeltaPath(new Path(f.toString() + DELETE_SUFFIX));
      FSDataOutputStream out = deltaFs.create(markerPath);
      out.close();
      LOG.info("Deleted file " + f + " using marker " + markerPath);
      return true;
    } else {
      return deltaFs.delete(convertToDeltaPath(f), true);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    // TODO: Recursive deletion is not handled. Not a usecase within Hudi.
    if (recursive) {
      LOG.warn("Recursive directory deletion is not supported: path=" + f);
    }
    return delete(f);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    // writes only go to the delta directory
    return deltaFs.mkdirs(convertToDeltaPath(f), permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    try {
      return convertToBasePath(deltaFs.getFileStatus(convertToDeltaPath(f)));
    } catch (FileNotFoundException e) {
      if (!deltaFs.exists(convertToDeltaPath(new Path(f.toString() + DELETE_SUFFIX)))) {
        return baseFs.getFileStatus(f);
      } else {
        throw new FileNotFoundException(f.getName());
      }
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Path origPath = file.getPath();
    try {
      Path convertedPath = convertToDeltaPath(origPath);
      file.setPath(convertedPath);
      return deltaFs.getFileBlockLocations(file, start, len);
    } catch (FileNotFoundException e) {
      file.setPath(origPath);
      return baseFs.getFileBlockLocations(file, start, len);
    }

  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
    try {
      return deltaFs.getFileBlockLocations(convertToDeltaPath(p), start, len);
    } catch (FileNotFoundException e) {
      return baseFs.getFileBlockLocations(p, start, len);
    }
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    try {
      return deltaFs.resolvePath(convertToDeltaPath(p));
    } catch (FileNotFoundException e) {
      return baseFs.resolvePath(p);
    }
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    try {
      return deltaFs.open(convertToDeltaPath(f));
    } catch (FileNotFoundException e) {
      return baseFs.open(f);
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.createNonRecursive(convertToDeltaPath(f), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.createNonRecursive(convertToDeltaPath(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    // writes only go to the delta directory
    return deltaFs.createNonRecursive(convertToDeltaPath(f), permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    // writes only go to the delta directory
    return deltaFs.createNewFile(convertToDeltaPath(f));
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    if (deltaFs.exists(convertToDeltaPath(f))) {
      return deltaFs.append(convertToDeltaPath(f));
    } else {
      // Copy the file from baseFs to deltaFS and append
      FileUtil.copy(baseFs.getFileSystem(), f, deltaFs.getFileSystem(), convertToDeltaPath(f), false,
          deltaFs.getFileSystem().getConf());
      return deltaFs.append(convertToDeltaPath(f));
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    if (deltaFs.exists(convertToDeltaPath(f))) {
      return deltaFs.append(convertToDeltaPath(f), bufferSize);
    } else {
      // Copy the file from baseFs to deltaFS and append
      FileUtil.copy(baseFs.getFileSystem(), f, deltaFs.getFileSystem(), convertToDeltaPath(f), false,
          deltaFs.getFileSystem().getConf());
      return deltaFs.append(convertToDeltaPath(f), bufferSize);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    if (deltaFs.exists(convertToDeltaPath(f))) {
      return deltaFs.append(convertToDeltaPath(f), bufferSize, progress);
    } else {
      // Copy the file from baseFs to deltaFS and append
      FileUtil.copy(baseFs.getFileSystem(), f, deltaFs.getFileSystem(), convertToDeltaPath(f), false,
          deltaFs.getFileSystem().getConf());
      return deltaFs.append(convertToDeltaPath(f), bufferSize, progress);
    }
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    // writes only go to the delta directory
    // TODO: psrcs may also be in the delta directory
    deltaFs.concat(convertToDeltaPath(trg), psrcs);
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    throw new HoodieException("deleteOnExit is not supported yet");
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    throw new HoodieException("cancelDeleteOnExit is not supported yet");
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return (baseFs.exists(f) && !deltaFs.exists(convertToDeltaPath(new Path(f.toString() + DELETE_SUFFIX))))
        || deltaFs.exists(convertToDeltaPath(f));
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    return getFileStatus(f).isDirectory();
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    return getFileStatus(f).isFile();
  }

  @Override
  public long getLength(Path f) throws IOException {
    try {
      return deltaFs.getLength(convertToDeltaPath(f));
    } catch (FileNotFoundException e) {
      return baseFs.getLength(f);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] baseStatus = {};
    try {
      baseStatus = baseFs.listStatus(f);
    } catch (FileNotFoundException e) {
      // directory does not exist in base
    }

    FileStatus[] deltaStatus = {};
    try {
      deltaStatus = deltaFs.listStatus(convertToDeltaPath(f));
    } catch (FileNotFoundException e) {
      // directory does not exist in delta
    }

    return mergeListStatus(baseStatus, deltaStatus);
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    FileStatus[] baseStatus = {};
    try {
      baseStatus = baseFs.listStatus(f, filter);
    } catch (FileNotFoundException e) {
      // directory does not exist in base
    }

    FileStatus[] deltaStatus = {};
    try {
      deltaStatus = deltaFs.listStatus(convertToDeltaPath(f), filter);
    } catch (FileNotFoundException e) {
      // directory does not exist in delta
    }

    return mergeListStatus(baseStatus, deltaStatus);
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    throw new HoodieException("listStatus(paths) is not supported yet");
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    throw new HoodieException("listStatus(paths, filter) is not supported yet");
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    FileStatus[] baseStatus = baseFs.globStatus(pathPattern);
    FileStatus[] deltaStatus = {};
    try {
      deltaStatus = deltaFs.globStatus(convertToDeltaPath(pathPattern));
    } catch (FileNotFoundException e) {
      // directory does not exist in delta
    }

    return mergeListStatus(baseStatus, deltaStatus);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    FileStatus[] baseStatus = baseFs.globStatus(pathPattern, filter);
    FileStatus[] deltaStatus = {};
    try {
      deltaStatus = deltaFs.globStatus(convertToDeltaPath(pathPattern), filter);
    } catch (FileNotFoundException e) {
      // directory does not exist in delta
    }

    return mergeListStatus(baseStatus, deltaStatus);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    throw new HoodieException("listLocatedStatus is not supported yet");
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    LinkedList<FileStatus> baseStatusList = new LinkedList<>();
    LinkedList<FileStatus> deltaStatusList = new LinkedList<>();
    RemoteIterator<LocatedFileStatus> itr;

    try {
      itr = baseFs.listFiles(f, recursive);
      while (itr.hasNext()) {
        FileStatus status = itr.next();
        baseStatusList.add(status);
      }
    } catch (FileNotFoundException e) {
      // directory does not exist in base
    }

    try {
      itr = deltaFs.listFiles(convertToDeltaPath(f), recursive);
      while (itr.hasNext()) {
        FileStatus status = itr.next();
        deltaStatusList.add(status);
      }
    } catch (FileNotFoundException e) {
      // directory does not exist in delta
    }

    FileStatus[] mergedStatus = mergeListStatus(baseStatusList.toArray(new FileStatus[0]), deltaStatusList.toArray(new FileStatus[0]));

    return new RemoteIterator<LocatedFileStatus>() {
      private int index = 0;

      @Override
      public boolean hasNext() throws IOException {
        return index < mergedStatus.length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more entry in " + f);
        }

        FileStatus result = mergedStatus[index++];
        BlockLocation[] locs = null;
        if (result.isFile()) {
          locs = getFileBlockLocations(result.getPath(), 0, result.getLen());
        }
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  private FileStatus[] mergeListStatus(FileStatus[] baseStatus, FileStatus[] deltaStatus) {
    Set<String> deltaFileNames = Arrays.stream(deltaStatus).map(s -> s.getPath().getName())
        .collect(Collectors.toSet());

    Stream<FileStatus> s1 = Stream.of(baseStatus)
        .filter(s -> {
          if (s.isDirectory()) {
            return true;
          }

          boolean matched = false;
          String filename = s.getPath().getName();
          try {
            // data files
            String ts = FSUtils.getCommitTime(filename);
            matched = ts.matches("\\d{14}$");  // yyyyMMddHHmmss
            if (matched && HoodieTimeline.compareTimestamps(ts, HoodieTimeline.GREATER_THAN, maxFileTimestampDatafiles)) {
              LOG.info("Filtering out file " + s + " with ts > maxFileTimestampDataFiles (" + ts + " > " + maxFileTimestampDatafiles);
              return false;
            }
            if (matched) {
              return true;
            }
          } catch (ArrayIndexOutOfBoundsException e) {
            // expected for non data files
          } catch (Exception e) {
            LOG.error("Failed to match file " + s, e);
          }

          try {
            // instants
            String ts = FSUtils.getCommitFromCommitFile(filename);
            matched = ts.matches("\\d{14}$"); // yyyyMMddHHmmss
            if (matched && HoodieTimeline.compareTimestamps(ts, HoodieTimeline.GREATER_THAN, maxFileTimestamp)) {
              LOG.info("Filtering out file " + s + " with timestamp > maxFileTimestamp (" + ts + " > " + maxFileTimestamp);
              return false;
            }
            if (matched) {
              return true;
            }
          } catch (ArrayIndexOutOfBoundsException e) {
            // expected for non instant files
          } catch (Exception e) {
            LOG.error("Failed to match file " + s, e);
          }

          if (!matched && maxFileTimestamp != null && !filename.equals(".hoodie_partition_metadata")) {
            LOG.warn("Could not find timestamp from file " + filename);
          }

          return true;
        })
        .filter(s -> !(deltaFileNames.contains(s.getPath().getName()) || deltaFileNames.contains(s.getPath().getName() + DELETE_SUFFIX)));

    Stream<FileStatus> s2 = Stream.of(deltaStatus).filter(s -> !s.getPath().getName().endsWith(DELETE_SUFFIX))
        .map(s -> convertToBasePath(s));

    return Stream.concat(s1, s2).toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    // writes only go to the delta directory
    return deltaFs.mkdirs(convertToDeltaPath(f));
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.copyFromLocalFile(src, convertToDeltaPath(dst));
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.moveFromLocalFile(srcs, convertToDeltaPath(dst));
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.moveFromLocalFile(src, convertToDeltaPath(dst));
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.copyFromLocalFile(delSrc, src, convertToDeltaPath(dst));
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.copyFromLocalFile(delSrc, overwrite, srcs, convertToDeltaPath(dst));
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.copyFromLocalFile(delSrc, overwrite, src, convertToDeltaPath(dst));
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    // writes only go to the delta directory
    deltaFs.copyFromLocalFile(src, convertToDeltaPath(dst));
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    try {
      deltaFs.moveToLocalFile(convertToDeltaPath(src), dst);
    } catch (FileNotFoundException e) {
      baseFs.moveToLocalFile(src, dst);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    try {
      deltaFs.copyToLocalFile(delSrc, convertToDeltaPath(src), dst);
    } catch (FileNotFoundException e) {
      baseFs.copyToLocalFile(delSrc, src, dst);
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
    try {
      deltaFs.copyToLocalFile(delSrc, convertToDeltaPath(src), dst, useRawLocalFileSystem);
    } catch (FileNotFoundException e) {
      baseFs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    try {
      deltaFs.access(convertToDeltaPath(path), mode);
    } catch (FileNotFoundException e) {
      baseFs.access(path, mode);
    }
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
    // writes only go to the delta directory
    deltaFs.createSymlink(target, convertToDeltaPath(link), createParent);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    try {
      return convertToBasePath(deltaFs.getFileLinkStatus(convertToDeltaPath(f)));
    } catch (FileNotFoundException e) {
      return baseFs.getFileLinkStatus(f);
    }
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    try {
      return deltaFs.getLinkTarget(f);
    } catch (FileNotFoundException e) {
      return baseFs.getLinkTarget(convertToDeltaPath(f));
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    try {
      return deltaFs.getStatus(convertToDeltaPath(p));
    } catch (FileNotFoundException e) {
      return baseFs.getStatus(p);
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    try {
      deltaFs.setPermission(convertToDeltaPath(p), permission);
    } catch (FileNotFoundException e) {
      baseFs.setPermission(p, permission);
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    try {
      deltaFs.setOwner(convertToDeltaPath(p), username, groupname);
    } catch (FileNotFoundException e) {
      baseFs.setOwner(p, username, groupname);
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    try {
      deltaFs.setTimes(convertToDeltaPath(p), mtime, atime);
    } catch (FileNotFoundException e) {
      baseFs.setTimes(p, mtime, atime);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName) throws IOException {
    try {
      return deltaFs.createSnapshot(convertToDeltaPath(path), snapshotName);
    } catch (FileNotFoundException e) {
      return baseFs.createSnapshot(path, snapshotName);
    }
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
    try {
      deltaFs.renameSnapshot(convertToDeltaPath(path), snapshotOldName, snapshotNewName);
    } catch (FileNotFoundException e) {
      baseFs.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName) throws IOException {
    try {
      deltaFs.deleteSnapshot(convertToDeltaPath(path), snapshotName);
    } catch (FileNotFoundException e) {
      baseFs.deleteSnapshot(path, snapshotName);
    }
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      deltaFs.modifyAclEntries(convertToDeltaPath(path), aclSpec);
    } catch (FileNotFoundException e) {
      baseFs.modifyAclEntries(path, aclSpec);
    }
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      deltaFs.removeAclEntries(convertToDeltaPath(path), aclSpec);
    } catch (FileNotFoundException e) {
      baseFs.removeAclEntries(path, aclSpec);
    }
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    try {
      deltaFs.removeDefaultAcl(convertToDeltaPath(path));
    } catch (FileNotFoundException e) {
      baseFs.removeDefaultAcl(path);
    }
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    try {
      deltaFs.removeAcl(convertToDeltaPath(path));
    } catch (FileNotFoundException e) {
      baseFs.removeAcl(path);
    }
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    try {
      deltaFs.setAcl(convertToDeltaPath(path), aclSpec);
    } catch (FileNotFoundException e) {
      baseFs.setAcl(path, aclSpec);
    }
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    try {
      return deltaFs.getAclStatus(convertToDeltaPath(path));
    } catch (FileNotFoundException e) {
      return baseFs.getAclStatus(path);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    try {
      deltaFs.setXAttr(convertToDeltaPath(path), name, value);
    } catch (FileNotFoundException e) {
      baseFs.setXAttr(path, name, value);
    }
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    try {
      deltaFs.setXAttr(convertToDeltaPath(path), name, value, flag);
    } catch (FileNotFoundException e) {
      baseFs.setXAttr(path, name, value, flag);
    }
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    try {
      return deltaFs.getXAttr(convertToDeltaPath(path), name);
    } catch (FileNotFoundException e) {
      return baseFs.getXAttr(path, name);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    try {
      return deltaFs.getXAttrs(convertToDeltaPath(path));
    } catch (FileNotFoundException e) {
      return baseFs.getXAttrs(path);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    try {
      return deltaFs.getXAttrs(convertToDeltaPath(path), names);
    } catch (FileNotFoundException e) {
      return baseFs.getXAttrs(path, names);
    }
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    try {
      return deltaFs.listXAttrs(convertToDeltaPath(path));
    } catch (FileNotFoundException e) {
      return baseFs.listXAttrs(path);
    }
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    try {
      deltaFs.removeXAttr(convertToDeltaPath(path), name);
    } catch (FileNotFoundException e) {
      baseFs.removeXAttr(path, name);
    }
  }

  @Override
  public Path makeQualified(Path path) {
    try {
      if (deltaFs.exists(convertToDeltaPath(path))) {
        return deltaFs.makeQualified(path);
      } else {
        return baseFs.makeQualified(path);
      }
    } catch (IOException e) {
      e.printStackTrace();
      return path;
    }
  }

  @Override
  public URI getUri() {
    return baseFs.getUri();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    baseFs.setWorkingDirectory(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return baseFs.getWorkingDirectory();
  }

  @Override
  public long getDefaultBlockSize() {
    return baseFs.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return baseFs.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication() {
    return baseFs.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return baseFs.getDefaultReplication(path);
  }

  @Override
  public long getBytesWritten(Path file) {
    // writes only go to the delta directory
    return deltaFs.getBytesWritten(file);
  }

  @Override
  public Configuration getConf() {
    return baseFs.getConf();
  }
}
