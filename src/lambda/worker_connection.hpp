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
#ifndef GRAPHLAB_LAMBDA_WORKER_CONNECTION_HPP
#define GRAPHLAB_LAMBDA_WORKER_CONNECTION_HPP

#include <mutex>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/signal.h>
#include <unistd.h>
#include<cppipc/client/comm_client.hpp>

namespace graphlab {

namespace lambda {

/**
 * Manages a connection to a spawned lambda worker.
 *
 * When the connection object is destroyed, the corresponding
 * worker process will be killed.
 */
template<typename ProxyType>
class worker_connection {
 public:
  worker_connection(size_t pid,
                    std::string address,
                    cppipc::comm_client* client)
    : m_pid(pid), m_address(address), m_client(client) {
      m_proxy.reset(new ProxyType(*m_client));
    }

  ~worker_connection() noexcept {
    try {
      m_client->stop();
    } catch (...) {
      logstream(LOG_ERROR) << "Fail stopping worker connection to process pid: "
                           << m_pid << std::endl;
    }
    if (!m_proxy.unique()) {
      logstream(LOG_ERROR) << "Worker proxy " << m_proxy
                           << " not unique" << std::endl;
    }
    m_proxy.reset();
    kill(m_pid, SIGKILL);
    int status;
    waitpid(m_pid, &status, 0);
  }

  std::shared_ptr<ProxyType> get_proxy() {
    return m_proxy;
  }

  pid_t get_pid() {
    return m_pid;
  }

  std::string get_address() {
    return m_address;
  }

 private:
  pid_t m_pid;
  std::string m_address;
  std::unique_ptr<cppipc::comm_client> m_client;
  std::shared_ptr<ProxyType> m_proxy;
};


} // end of lambda namespace
} // end of graphlab namespace
#endif
