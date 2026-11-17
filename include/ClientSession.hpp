#pragma once

#include <string>
#include <string_view>

#include "Cache.hpp"
#include "Config.hpp"
#include "Fetcher.hpp"
#include "Http.hpp"
#include "Socket.hpp"
#include "ThreadPool.hpp"

class ClientSession {
 public:
  ClientSession(Socket socket,
                Cache& cache,
                Fetcher& fetcher,
                ThreadPool& fetchPool,
                const StopSignal& stop,
                const Config& config);

  void run();
  void reject(int status, std::string_view message);

 private:
  void handle();
  void serveCached(const HttpRequest& request);
  void serveDirect(const HttpRequest& request);
  void respond(int status, std::string_view message);
  void sendError(int status, std::string_view message);

  Socket socket_;
  Cache& cache_;
  Fetcher& fetcher_;
  ThreadPool& fetchPool_;
  const StopSignal& stop_;
  const Config& config_;
  bool headSent_{false};
  std::string url_;
};
