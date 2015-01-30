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
#ifndef GRAPHLAB_LAMBDA_WORKER_POOL_HPP
#define GRAPHLAB_LAMBDA_WORKER_POOL_HPP

#include<lambda/worker_connection.hpp>
#include<parallel/lambda_omp.hpp>
#include<logger/assertions.hpp>
#include<boost/filesystem/operations.hpp>
#include<boost/filesystem/path.hpp>
#include<condition_variable>
#include<mutex>
#include<fileio/fs_utils.hpp>
#include<fileio/temp_files.hpp>

namespace graphlab {

namespace lambda {

/**
 * Create a worker process using given binary path and the worker_address.
 *
 * Returns a pointer to the worker_connection or throws an exception if failed.
 */
template<typename ProxyType>
std::shared_ptr<worker_connection<ProxyType>> spawn_worker(std::string worker_binary,
                                                           std::string worker_address,
                                                           int connection_timeout) {
 logstream(LOG_INFO) << "Start lambda worker at " << worker_address
                    << " using binary: " << worker_binary << std::endl;

 namespace fs = boost::filesystem;
  if (!fs::exists(fs::path(worker_binary))) {
    throw std::string("lambda_worker executable: ") + worker_binary + " not found.";
  }

  logstream(LOG_INFO) << "Start lambda worker at " << worker_address
                      << " using binary: " << worker_binary << std::endl;
#ifdef __APPLE__
  pid_t pid = fork();
#else
  pid_t pid = vfork();
#endif
  if (pid == 0){
    // In child proc
    execl(worker_binary.c_str(),
          worker_binary.c_str(),
          worker_address.c_str(), NULL); 
    _exit(0);
  } else {
    // In master proc
    cppipc::comm_client* cli = NULL;
    logstream(LOG_INFO) << "Worker pid = " << pid << std::endl;
    if (pid < 0) {
      logstream(LOG_ERROR) << "Fail forking lambda worker at address: " + worker_address
                           << "Error: " << strerror(errno) << std::endl;
    } else  {
      try {
        cli = new cppipc::comm_client({}, worker_address, connection_timeout);
        cppipc::reply_status status = cli->start();
        if (status == cppipc::reply_status::OK) {
          logstream(LOG_INFO) << "Connected to worker at " << worker_address << std::endl;
        } else {
          logstream(LOG_INFO) << "Fail connecting to worker at " << worker_address
                              << ". Status: " 
                              << cppipc::reply_status_to_string(status) << std::endl;
        }
      } catch (std::string error) {
        logstream(LOG_ERROR) << error << std::endl;
        if (cli != NULL) {
          delete cli;
          cli = NULL;
        }
      }
    }

    if (cli == NULL) {
      throw std::string("Fail creating lambda worker.");
    } else {
      return std::make_shared<worker_connection<ProxyType>>(pid, worker_address, cli);
    }
  }
}

/**
 * Forward declaration. RAII for worker proxy allocation.
 */
template<typename ProxyType>
class worker_guard;

/**
 * Worker pool manages a pool of spawned worker connections.
 *
 * The worker pool is fix sized, and all the worker processes
 * are created at the creation of worker pool, and destroyed
 * together with the worker pool.
 */
template<typename ProxyType>
class worker_pool {

 public:
  typedef std::shared_ptr<worker_connection<ProxyType>> worker_conn_ptr;

 public:
  /**
   * Creates a worker pool of given size.
   * The actual pool size may be smaller due to resource limitation.
   * Throws exception if zero worker is started.
   */
  worker_pool(size_t nworkers,
              std::string worker_binary,
              std::vector<std::string> worker_addresses={},
              int connection_timeout=3) {
    if (worker_addresses.empty()) {
      for (size_t i = 0; i < nworkers; ++i)
        worker_addresses.push_back("ipc://" + get_temp_name());
    }
    ASSERT_EQ(worker_addresses.size(), nworkers);
    ASSERT_GT(nworkers, 0);

    m_worker_binary = worker_binary;
    m_connection_timeout = connection_timeout; 

    parallel_for(0, nworkers, [&](size_t i) {
      try {
        auto worker_conn = spawn_worker<ProxyType>(worker_binary, worker_addresses[i], m_connection_timeout);

        std::unique_lock<std::mutex> lck(mtx);
        connections.push_back(worker_conn);
        available_workers.push_back(worker_conn->get_proxy());
        proxy_to_connection[worker_conn->get_proxy()] = connections.size()-1;
        m_worker_addresses.push_back(worker_addresses[i]);
      } catch(std::string e) {
        logstream(LOG_ERROR) << e << std::endl;
      } catch(const char* e) {
        logstream(LOG_ERROR) << e << std::endl;
      } catch (...) {
        // spawning worker might fail. The acutal pool size may have
        // less worker or no worker at all.
      }
    });

    if (num_workers() == 0) {
      log_and_throw("Unable to evaluate lambdas. lambda workers did not start");
    } else if (num_workers() < nworkers) {
      logprogress_stream << "Less than " << nworkers << " successfully started. "
                         << "Using only " << num_workers()  << " workers." << std::endl;
      logprogress_stream << "All operations will proceed as normal, but "
                         << "lambda operations will not be able to use all "
                         << "available cores." << std::endl;
      logprogress_stream << "To help us diagnose this issue, please send "
                         << "the log file to product-feedback@dato.com." << std::endl;
      logprogress_stream << "(The location of the log file is printed at the "
                         << "start of the GraphLab server)."  << std::endl;
      logstream(LOG_ERROR) << "Less than " << nworkers << " successfully started."
                           << "Using only " << num_workers() << std::endl;
    }
  }

  ~worker_pool() {
    available_workers.clear();
    proxy_to_connection.clear();
    for (auto& conn : connections) {
      deleted_connections.push_back(std::move(conn));
    }
    parallel_for(0, deleted_connections.size(), [&](size_t i) {
      deleted_connections[i].reset();
    });
  }

  /**
   * Returns a worker proxy that is available, block till there is worker available.
   * Thread-safe.
   */
  std::shared_ptr<ProxyType> get_worker() {
    std::unique_lock<std::mutex> lck(mtx);
    while (available_workers.empty()) {
      cv.wait(lck);
    }
    auto proxy = available_workers.front();
    available_workers.pop_front();
    return proxy;
  }

  /**
   * Put a worker proxy back to avaiable queue. Has no effect if the worker proxy
   * is already available.
   * Thread-safe.
   */
  void release_worker(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<std::mutex> lck(mtx);
    for (auto& p : available_workers) {
      if (p == worker_proxy)
        return;
    }
    available_workers.push_back(worker_proxy);
    lck.unlock();
    cv.notify_one();
  }

  /**
   * Returns a worker_guard for the given worker proxy.
   * When the worker_guard goes out of the scope, the guarded
   * worker_proxy will be automatically released.
   */
  std::shared_ptr<worker_guard<ProxyType>> get_worker_guard(std::shared_ptr<ProxyType> worker_proxy) {
    return std::make_shared<worker_guard<ProxyType>>(*this, worker_proxy);
  }

  /**
   * Block until all workers are avaiable.
   */
  void barrier() {
    std::unique_lock<std::mutex> lck(mtx);
    while (available_workers.size() != connections.size()) {
      cv.wait(lck);
    }
  }

  /**
   * Return all the workers in the pool. Calls barrier() first.
   */
  std::vector<std::shared_ptr<ProxyType>> get_all_workers() {
    barrier();
    return std::vector<std::shared_ptr<ProxyType>>(available_workers.begin(), available_workers.end());
  }

  /**
   * Returns number of workers in the pool.
   */
  size_t num_workers() const {
    return connections.size();
  };

  /**
   * Returns number of available workers in the pool. 
   */
  size_t num_available_workers() {
    std::unique_lock<std::mutex> lck(mtx);
    return available_workers.size();
  }

  /**
   * Return the pid associated with a worker proxy.
   * Return -1 if the worker proxy not exist.
   */
  pid_t get_pid(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<std::mutex> lck(mtx);
    if (proxy_to_connection.count(worker_proxy)) {
      auto conn = connections[proxy_to_connection[worker_proxy]];
      return conn->get_pid();
    } else {
      return -1;
    }
  }

  /**
   * Restart the worker
   */
  void restart_worker(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<std::mutex> lck(mtx);
    DASSERT_TRUE(proxy_to_connection.count(worker_proxy) == 1);
    size_t id = proxy_to_connection[worker_proxy];
    logstream(LOG_INFO) << "Restart lambda worker " << worker_proxy << std::endl;
    auto& conn = connections[id];
    auto address = conn->get_address();
    logstream(LOG_INFO) << "Old worker pid: " << conn->get_pid()
                        << " address: " << address;
    // remove the old ipc socket file if it still exists
    size_t pos = address.find("ipc://");
    if (pos != std::string::npos) {
      fileio::delete_path(address.substr(6));
    }

    // Move the dead connection to deleted_connection
    // the reason we cannot destroy the old connection object directly 
    // is because the connection objects holds the unique ownership of
    // the comm_client object, however, at this point there might still
    // be shared pointers of the proxy object flying around and the client
    // object must exist for the proxy object to be properly cleaned up.
    deleted_connections.push_back(std::move(conn));

    // spawn new worker
    try {
      conn = (spawn_worker<ProxyType>(m_worker_binary, address, m_connection_timeout));
      logstream(LOG_INFO) << "New worker pid: " << conn->get_pid()
                          << " address: " << address << std::endl;

      logstream(LOG_INFO) << "Successfully restarted lambda worker. New proxy: "
                          << conn->get_proxy() << std::endl;

      // update bookkeepers
      proxy_to_connection.erase(proxy_to_connection.find(worker_proxy));
      proxy_to_connection[conn->get_proxy()] = id;
      auto iter = std::find(available_workers.begin(), available_workers.end(), worker_proxy);
      if (iter != available_workers.end()) {
        available_workers.erase(iter);
      }
      available_workers.push_back(conn->get_proxy());
      lck.unlock();
      cv.notify_one();
    } catch (std::exception e){
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e.what() << std::endl;
    } catch (std::string e){
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e << std::endl;
    } catch (const char* e) {
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e << std::endl;
    } catch (...) {
      logstream(LOG_INFO) << "Fail restarting lambda worker. Unknown error." << std::endl;
    }
  }

 private:
  std::vector<worker_conn_ptr> connections;

  std::vector<worker_conn_ptr> deleted_connections;

  std::map<std::shared_ptr<ProxyType>, size_t> proxy_to_connection;

  std::deque<std::shared_ptr<ProxyType>> available_workers;

  std::string m_worker_binary;
  std::vector<std::string> m_worker_addresses;
  int m_connection_timeout;

  std::mutex mtx;
  std::condition_variable cv;
}; // end of worker_pool


/**
 * RAII for allocating worker proxy.
 * When the worker_guard is destoryed, the guarded worker
 * be released by the worker_pool.
 */
template<typename ProxyType>
class worker_guard {
 public:
   worker_guard(worker_pool<ProxyType>& worker_pool_,
                std::shared_ptr<ProxyType> worker_proxy_)
     : m_pool(worker_pool_), m_proxy(worker_proxy_) { }

   ~worker_guard() {
     pid_t pid = m_pool.get_pid(m_proxy);
     int status;
     pid_t result = waitpid(pid, &status, WNOHANG);
     if (result == 0) {
       // Child still alive
       m_pool.release_worker(m_proxy);
     } else {
       logstream(LOG_ERROR) << "Process of worker " << m_proxy << " has crashed" << std::endl;
       // restart the worker process
       m_pool.restart_worker(m_proxy);
     }
   }

 private:
   worker_pool<ProxyType>& m_pool;
   std::shared_ptr<ProxyType> m_proxy;
};

} // end of lambda namespace
} // end of graphlab namespace
#endif
