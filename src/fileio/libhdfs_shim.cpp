/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
// libhdfs shim library
#include <globals/global_constants.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <vector>
#include <string>
#ifdef HAS_HADOOP
#include <dlfcn.h>
extern  "C" {
#include <hdfs.h>
}

extern  "C" {
  static void* libhdfs_handle = NULL;
  bool dlopen_fail = false;
  /*
   * All the shim pointers
   */
  static hdfsFS (*ptr_hdfsConnectAsUser)(const char* host, tPort port, const char *user) = NULL;
  static hdfsFS (*ptr_hdfsConnect)(const char* host, tPort port) = NULL;
  static int (*ptr_hdfsDisconnect)(hdfsFS fs) = NULL;
  static hdfsFile (*ptr_hdfsOpenFile)(hdfsFS fs, const char* path, int flags,
                        int bufferSize, short replication, tSize blocksize) = NULL;
  static int (*ptr_hdfsCloseFile)(hdfsFS fs, hdfsFile file) = NULL;
  static int (*ptr_hdfsExists)(hdfsFS fs, const char *path) = NULL;
  static int (*ptr_hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos) = NULL;
  static tOffset (*ptr_hdfsTell)(hdfsFS fs, hdfsFile file) = NULL;
  static tSize (*ptr_hdfsRead)(hdfsFS fs, hdfsFile file, void* buffer, tSize length) = NULL;
  static tSize (*ptr_hdfsPread)(hdfsFS fs, hdfsFile file, tOffset position,
                  void* buffer, tSize length) = NULL;
  static tSize (*ptr_hdfsWrite)(hdfsFS fs, hdfsFile file, const void* buffer,
                  tSize length) = NULL;
  static int (*ptr_hdfsFlush)(hdfsFS fs, hdfsFile file) = NULL;
  static int (*ptr_hdfsAvailable)(hdfsFS fs, hdfsFile file) = NULL;
  static int (*ptr_hdfsCopy)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) = NULL;
  static int (*ptr_hdfsMove)(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) = NULL;
  static int (*ptr_hdfsDelete)(hdfsFS fs, const char* path, int recursive) = NULL;
  static int (*ptr_hdfsRename)(hdfsFS fs, const char* oldPath, const char* newPath) = NULL;
  static char* (*ptr_hdfsGetWorkingDirectory)(hdfsFS fs, char *buffer, size_t bufferSize) = NULL;
  static int (*ptr_hdfsSetWorkingDirectory)(hdfsFS fs, const char* path) = NULL;
  static int (*ptr_hdfsCreateDirectory)(hdfsFS fs, const char* path) = NULL;
  static int (*ptr_hdfsSetReplication)(hdfsFS fs, const char* path, int16_t replication) = NULL;
  static hdfsFileInfo* (*ptr_hdfsListDirectory)(hdfsFS fs, const char* path,
                                  int *numEntries) = NULL;
  static hdfsFileInfo* (*ptr_hdfsGetPathInfo)(hdfsFS fs, const char* path) = NULL;
  static void (*ptr_hdfsFreeFileInfo)(hdfsFileInfo *hdfsFileInfo, int numEntries) = NULL;
  static char*** (*ptr_hdfsGetHosts)(hdfsFS fs, const char* path,
                       tOffset start, tOffset length) = NULL;
  static void (*ptr_hdfsFreeHosts)(char ***blockHosts) = NULL;
  static tOffset (*ptr_hdfsGetDefaultBlockSize)(hdfsFS fs) = NULL;
  static tOffset (*ptr_hdfsGetCapacity)(hdfsFS fs) = NULL;
  static tOffset (*ptr_hdfsGetUsed)(hdfsFS fs) = NULL;
  static int (*ptr_hdfsChown)(hdfsFS fs, const char* path, const char *owner, const char *group) = NULL;
  static int (*ptr_hdfsChmod)(hdfsFS fs, const char* path, short mode) = NULL;
  static int (*ptr_hdfsUtime)(hdfsFS fs, const char* path, tTime mtime, tTime atime) = NULL;

  static void connect_shim() {
    static bool shim_attempted = false;
    if (shim_attempted == false) {
      shim_attempted = true;
      const char *err_msg;

      std::vector<std::string> potential_paths = {
        // find one in the unity_server directory
        graphlab::GLOBALS_MAIN_PROCESS_PATH + "/libhdfs.so",
        // find one in the local directory
        "./libhdfs.so",
        // special handling for internal build path locations
        graphlab::GLOBALS_MAIN_PROCESS_PATH + "/../../../../deps/local/lib/libhdfs.so",
        // find a global libhdfs.so
        "libhdfs.so",
      };

      // We don't want to freak customers out with failure messages as we try
      // to load these libraries.  There's going to be a few in the normal
      // case. Print them if we really didn't find libhdfs.so.
      std::vector<std::string> error_messages;

      for(auto &i : potential_paths) {
        logstream(LOG_INFO) << "Trying " << i << std::endl;
        libhdfs_handle = dlopen(i.c_str(), RTLD_NOW | RTLD_LOCAL);

        if(libhdfs_handle != NULL) {
          logstream(LOG_INFO) << "Success!" << std::endl;
          break;
        } else {
          err_msg = dlerror();
          if(err_msg != NULL) {
            error_messages.push_back(std::string(err_msg));
          } else {
            error_messages.push_back(std::string("dlerror returned NULL"));
          }
        }
      }

      if (libhdfs_handle == NULL) {
        logstream(LOG_INFO) << "Unable to load libhdfs.so" << std::endl;
        dlopen_fail = true;
        for(size_t i = 0; i < potential_paths.size(); ++i) {
          logstream(LOG_INFO) << error_messages[i] << std::endl;
        }
      }
    }
  }

  static void* get_symbol(const char* symbol) {
    connect_shim();
    if (dlopen_fail || libhdfs_handle == NULL) return NULL;
    return dlsym(libhdfs_handle, symbol);
  }

  hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char *user) {
    if (!ptr_hdfsConnectAsUser) *(void**)(&ptr_hdfsConnectAsUser) = get_symbol("hdfsConnectAsUser");
    if (ptr_hdfsConnectAsUser) return ptr_hdfsConnectAsUser(host, port, user);
    else return NULL;
  }


  hdfsFS hdfsConnect(const char* host, tPort port) {
    if (!ptr_hdfsConnect) { 
      *(void**)(&ptr_hdfsConnect) = get_symbol("hdfsConnect");
    }
    if (ptr_hdfsConnect) { 
      auto x = ptr_hdfsConnect(host, port);
      if (x == NULL) {
        logstream(LOG_INFO) << "hdfsConnect to " << host << ":" << port << " Failed" << std::endl;
      } 
      return x;
    } else {
      logstream(LOG_INFO) << "hdfsConnect failed because the hdfsConnect symbol cannot be found" << std::endl;
      return NULL;
    }
  }


  int hdfsDisconnect(hdfsFS fs)  {
    if (!ptr_hdfsDisconnect) *(void**)(&ptr_hdfsDisconnect) = get_symbol("hdfsDisconnect");
    if (ptr_hdfsDisconnect) return ptr_hdfsDisconnect(fs);
    else return 0;
  }

  hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                        int bufferSize, short replication, tSize blocksize) {
    if (!ptr_hdfsOpenFile) *(void**)(&ptr_hdfsOpenFile) = get_symbol("hdfsOpenFile");
    if (ptr_hdfsOpenFile) return ptr_hdfsOpenFile(fs, path, flags, bufferSize, replication, blocksize);
    else return NULL;
  }


  int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
    if(!ptr_hdfsCloseFile) *(void**)(&ptr_hdfsCloseFile) = get_symbol("hdfsCloseFile");
    if (ptr_hdfsCloseFile) return ptr_hdfsCloseFile(fs, file);
    else return 0;
  }


  int hdfsExists(hdfsFS fs, const char *path) {
    if(!ptr_hdfsExists) *(void**)(&ptr_hdfsExists) = get_symbol("hdfsExists");
    if (ptr_hdfsExists) return ptr_hdfsExists(fs, path);
    else return 0;
  }


  int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
    if(!ptr_hdfsSeek) *(void**)(&ptr_hdfsSeek) = get_symbol("hdfsSeek");
    if (ptr_hdfsSeek) return ptr_hdfsSeek(fs, file, desiredPos);
    else return 0;
  }


  tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
    if(!ptr_hdfsTell) *(void**)(&ptr_hdfsTell) = get_symbol("hdfsTell");
    if (ptr_hdfsTell) return ptr_hdfsTell(fs, file);
    else return 0;
  }


  tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
    if(!ptr_hdfsRead) *(void**)(&ptr_hdfsRead) = get_symbol("hdfsRead");
    if (ptr_hdfsRead) return ptr_hdfsRead(fs, file, buffer, length);
    else return 0;
  }


  tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length) {
    if(!ptr_hdfsPread) *(void**)(&ptr_hdfsPread) = get_symbol("hdfsPread");
    if (ptr_hdfsPread) return ptr_hdfsPread(fs, file, position, buffer, length);
    else return 0;
  }


  tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length) {
    if(!ptr_hdfsWrite) *(void**)(&ptr_hdfsWrite) = get_symbol("hdfsWrite");
    if (ptr_hdfsWrite) return ptr_hdfsWrite(fs, file, buffer, length);
    else return 0;
  }


  int hdfsFlush(hdfsFS fs, hdfsFile file) {
    if(!ptr_hdfsFlush) *(void**)(&ptr_hdfsFlush) = get_symbol("hdfsFlush");
    if (ptr_hdfsFlush) return ptr_hdfsFlush(fs, file);
    else return 0;
  }


  int hdfsAvailable(hdfsFS fs, hdfsFile file) {
    if(!ptr_hdfsAvailable) *(void**)(&ptr_hdfsAvailable) = get_symbol("hdfsAvailable");
    if (ptr_hdfsAvailable) return ptr_hdfsAvailable(fs, file);
    else return 0;
  }


  int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
    if(!ptr_hdfsCopy) *(void**)(&ptr_hdfsCopy) = get_symbol("hdfsCopy");
    if (ptr_hdfsCopy) return ptr_hdfsCopy(srcFS, src, dstFS, dst);
    else return 0;
  }


  int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
    if(!ptr_hdfsMove) *(void**)(&ptr_hdfsMove) = get_symbol("hdfsMove");
    if (ptr_hdfsMove) return ptr_hdfsMove(srcFS, src, dstFS, dst);
    else return 0;
  }


  int hdfsDelete(hdfsFS fs, const char* path, int recursive) {
    if(!ptr_hdfsDelete) *(void**)(&ptr_hdfsDelete) = get_symbol("hdfsDelete");
    if (ptr_hdfsDelete) return ptr_hdfsDelete(fs, path, recursive);
    else return 0;
  }


  int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
    if(!ptr_hdfsRename) *(void**)(&ptr_hdfsRename) = get_symbol("hdfsRename");
    if (ptr_hdfsRename) return ptr_hdfsRename(fs, oldPath, newPath);
    else return 0;
  }


  char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize) {
    if(!ptr_hdfsGetWorkingDirectory) *(void**)(&ptr_hdfsGetWorkingDirectory) = get_symbol("hdfsGetWorkingDirectory");
    if (ptr_hdfsGetWorkingDirectory) return ptr_hdfsGetWorkingDirectory(fs, buffer, bufferSize);
    else return NULL;
  }


  int hdfsSetWorkingDirectory(hdfsFS fs, const char* path) {
    if(!ptr_hdfsSetWorkingDirectory) *(void**)(&ptr_hdfsSetWorkingDirectory) = get_symbol("hdfsSetWorkingDirectory");
    if (ptr_hdfsSetWorkingDirectory) return ptr_hdfsSetWorkingDirectory(fs, path);
    else return 0;
  }


  int hdfsCreateDirectory(hdfsFS fs, const char* path) {
    if(!ptr_hdfsCreateDirectory) *(void**)(&ptr_hdfsCreateDirectory) = get_symbol("hdfsCreateDirectory");
    if (ptr_hdfsCreateDirectory) return ptr_hdfsCreateDirectory(fs, path);
    else return 0;
  }


  int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication) {
    if(!ptr_hdfsSetReplication) *(void**)(&ptr_hdfsSetReplication) = get_symbol("hdfsSetReplication");
    if (ptr_hdfsSetReplication) return ptr_hdfsSetReplication(fs, path, replication);
    else return 0;
  }



  hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries) {
    if(!ptr_hdfsListDirectory) *(void**)(&ptr_hdfsListDirectory) = get_symbol("hdfsListDirectory");
    if (ptr_hdfsListDirectory) return ptr_hdfsListDirectory(fs, path, numEntries);
    else return NULL;
  }


  hdfsFileInfo* hdfsGetPathInfo(hdfsFS fs, const char* path) {
    if(!ptr_hdfsGetPathInfo) *(void**)(&ptr_hdfsGetPathInfo) = get_symbol("hdfsGetPathInfo");
    if (ptr_hdfsGetPathInfo) return ptr_hdfsGetPathInfo(fs, path);
    else return 0;
  }


  void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries) {
    if(!ptr_hdfsFreeFileInfo) *(void**)(&ptr_hdfsFreeFileInfo) = get_symbol("hdfsFreeFileInfo");
    if (ptr_hdfsFreeFileInfo) ptr_hdfsFreeFileInfo(hdfsFileInfo, numEntries);
  }


  char*** hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length) {
    if(!ptr_hdfsGetHosts) *(void**)(&ptr_hdfsGetHosts) = get_symbol("hdfsGetHosts");
    if (ptr_hdfsGetHosts) return ptr_hdfsGetHosts(fs, path, start, length);
    else return NULL;
  }


  void hdfsFreeHosts(char*** blockHosts) {
    if(!ptr_hdfsFreeHosts) *(void**)(&ptr_hdfsFreeHosts) = get_symbol("hdfsFreeHosts");
    if (ptr_hdfsFreeHosts) ptr_hdfsFreeHosts(blockHosts);
  }


  tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
    if(!ptr_hdfsGetDefaultBlockSize) *(void**)(&ptr_hdfsGetDefaultBlockSize) = get_symbol("hdfsGetDefaultBlockSize");
    if (ptr_hdfsGetDefaultBlockSize) return ptr_hdfsGetDefaultBlockSize(fs);
    else return 0;
  }


  tOffset hdfsGetCapacity(hdfsFS fs) {
    if(!ptr_hdfsGetCapacity) *(void**)(&ptr_hdfsGetCapacity) = get_symbol("hdfsGetCapacity");
    if (ptr_hdfsGetCapacity) return ptr_hdfsGetCapacity(fs);
    else return 0;
  }


  tOffset hdfsGetUsed(hdfsFS fs) {
    if(!ptr_hdfsGetUsed) *(void**)(&ptr_hdfsGetUsed) = get_symbol("hdfsGetUsed");
    if (ptr_hdfsGetUsed) return ptr_hdfsGetUsed(fs);
    else return 0;
  }

  int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group) {
    if(!ptr_hdfsChown) *(void**)(&ptr_hdfsChown) = get_symbol("hdfsChown");
    if (ptr_hdfsChown) return ptr_hdfsChown(fs, path, owner, group);
    else return 0;
  }

  int hdfsChmod(hdfsFS fs, const char* path, short mode) {
    if(!ptr_hdfsChmod) *(void**)(&ptr_hdfsChmod) = get_symbol("hdfsChmod");
    if (ptr_hdfsChmod) return ptr_hdfsChmod(fs, path, mode);
    else return 0;
  }

  int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
    if(!ptr_hdfsUtime) *(void**)(&ptr_hdfsUtime) = get_symbol("hdfsUtime");
    if (ptr_hdfsUtime) return ptr_hdfsUtime(fs, path, mtime, atime);
    else return 0;
  }

}
#endif
