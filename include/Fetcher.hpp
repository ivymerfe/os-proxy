#pragma once

#include <chrono>
#include <memory>

#include "Cache.hpp"
#include "Http.hpp"
#include "StopSignal.hpp"

class Fetcher {
 public:
  Fetcher(Cache& cache,
          const StopSignal& stop,
          std::chrono::milliseconds timeout)
      : cache_(cache), stop_(stop), timeout_(timeout) {}

  void run(const HttpRequest& request,
           const std::shared_ptr<CacheEntry>& entry);

 private:
  Cache& cache_;
  const StopSignal& stop_;
  std::chrono::milliseconds timeout_;
};
