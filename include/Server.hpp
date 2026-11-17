#pragma once

#include "Cache.hpp"
#include "Config.hpp"
#include "Fetcher.hpp"
#include "StopSignal.hpp"
#include "ThreadPool.hpp"
#include "UniqueFd.hpp"

class Server {
 public:
  Server(const Config& config, StopSignal& stop)
      : config_(config), stop_(stop) {}

  int run();

 private:
  UniqueFd createListener();
  void acceptLoop(Cache& cache,
                  Fetcher& fetcher,
                  ThreadPool& fetchPool,
                  ThreadPool& clientPool);
  void acceptOne(Cache& cache,
                 Fetcher& fetcher,
                 ThreadPool& fetchPool,
                 ThreadPool& clientPool);
  void handleDescriptorExhaustion();

  Config config_;
  StopSignal& stop_;
  UniqueFd listener_;
  UniqueFd spare_;
};
