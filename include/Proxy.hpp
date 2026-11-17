#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "Cache.hpp"
#include "Config.hpp"
#include "Http.hpp"
#include "Net.hpp"
#include "Runtime.hpp"

class ClientSession {
 public:
  ClientSession(Socket socket,
                Cache& cache,
                ThreadPool& fetchPool,
                const StopSignal& stop,
                const Config& config);

  void run();
  void reject(int status, std::string_view message);

 private:
  void handle();
  void serveCached(const HttpRequest& request);
  void serveDirect(const HttpRequest& request);
  void startFetch(const HttpRequest& request,
                  const std::shared_ptr<CacheEntry>& entry);
  void respond(int status, std::string_view message);
  void sendError(int status, std::string_view message);

  Socket socket_;
  Cache& cache_;
  ThreadPool& fetchPool_;
  const StopSignal& stop_;
  const Config& config_;
  bool headSent_{false};
  std::string url_;
};

class Server {
 public:
  Server(const Config& config, StopSignal& stop)
      : config_(config), stop_(stop) {}

  int run();

 private:
  UniqueFd createListener();
  void acceptLoop();
  void acceptOne();
  void handleDescriptorExhaustion();

  Config config_;
  StopSignal& stop_;
  UniqueFd listener_;
  UniqueFd spare_;
  std::unique_ptr<Cache> cache_;
  std::unique_ptr<ThreadPool> fetchPool_;
  std::unique_ptr<ThreadPool> clientPool_;
};
