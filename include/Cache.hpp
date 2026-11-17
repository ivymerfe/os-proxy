#pragma once

#include <chrono>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "CacheEntry.hpp"

class Cache {
 public:
  struct Acquired {
    std::shared_ptr<CacheEntry> entry;
    std::unique_ptr<EntryReader> reader;
    bool created;
  };

  Cache(std::size_t initialSize, std::size_t maxSize, std::chrono::seconds ttl);
  ~Cache();
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  Acquired acquire(const std::string& key);
  void remove(const std::shared_ptr<CacheEntry>& entry);
  void shutdown();
  std::size_t maxEntrySize() const noexcept { return budget_->maxSize(); }

 private:
  struct Slot {
    std::shared_ptr<CacheEntry> entry;
    std::list<std::string>::iterator lru;
  };

  void maintenanceLoop();
  void sweepExpired();
  void evict(std::size_t bytes);

  std::chrono::seconds ttl_;
  std::shared_ptr<CacheBudget> budget_;
  std::mutex mutex_;
  std::unordered_map<std::string, Slot> map_;
  std::list<std::string> lru_;
  std::thread maintainer_;
};
