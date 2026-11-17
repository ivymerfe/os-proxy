#include "Cache.hpp"

#include <algorithm>

#include "Logger.hpp"

Cache::Cache(std::size_t initialSize,
             std::size_t maxSize,
             std::chrono::seconds ttl)
    : ttl_(ttl), budget_(std::make_shared<CacheBudget>(initialSize, maxSize)) {
  maintainer_ = std::thread([this] { maintenanceLoop(); });
}

Cache::~Cache() {
  shutdown();
}

void Cache::shutdown() {
  budget_->stop();
  if (maintainer_.joinable()) {
    maintainer_.join();
  }
  std::lock_guard lock(mutex_);
  map_.clear();
  lru_.clear();
}

Cache::Acquired Cache::acquire(const std::string& key) {
  std::lock_guard lock(mutex_);
  const auto now = CacheEntry::Clock::now();
  const auto it = map_.find(key);
  if (it != map_.end()) {
    const auto entry = it->second.entry;
    if (!entry->expired(now, ttl_)) {
      if (auto reader = entry->openReader()) {
        lru_.splice(lru_.begin(), lru_, it->second.lru);
        return {entry, std::move(reader), false};
      }
    }
    lru_.erase(it->second.lru);
    map_.erase(it);
  }

  auto entry = std::make_shared<CacheEntry>(key, budget_);
  auto reader = entry->openReader();
  lru_.push_front(key);
  try {
    map_.emplace(key, Slot{entry, lru_.begin()});
  } catch (...) {
    lru_.pop_front();
    throw;
  }
  return {std::move(entry), std::move(reader), true};
}

void Cache::remove(const std::shared_ptr<CacheEntry>& entry) {
  std::lock_guard lock(mutex_);
  const auto it = map_.find(entry->key());
  if (it != map_.end() && it->second.entry == entry) {
    lru_.erase(it->second.lru);
    map_.erase(it);
  }
}

void Cache::sweepExpired() {
  const auto now = CacheEntry::Clock::now();
  std::lock_guard lock(mutex_);
  for (auto it = lru_.begin(); it != lru_.end();) {
    const auto slot = map_.find(*it);
    if (slot->second.entry->expired(now, ttl_) &&
        slot->second.entry->evictable()) {
      it = lru_.erase(it);
      map_.erase(slot);
    } else {
      ++it;
    }
  }
}

void Cache::evict(std::size_t bytes) {
  std::lock_guard lock(mutex_);
  std::size_t freed = 0;
  for (auto it = lru_.end(); it != lru_.begin() && freed < bytes;) {
    --it;
    const auto slot = map_.find(*it);
    if (!slot->second.entry->evictable()) {
      continue;
    }
    freed += slot->second.entry->size();
    it = lru_.erase(it);
    map_.erase(slot);
  }
}

void Cache::maintenanceLoop() {
  const auto period =
      std::max(std::chrono::milliseconds(1000),
               std::chrono::duration_cast<std::chrono::milliseconds>(ttl_) / 2);
  while (budget_->waitForWork(period)) {
    const std::uint64_t sequence = budget_->beginPass();
    try {
      sweepExpired();
      if (const std::size_t need = budget_->bytesToEvict(); need != 0) {
        evict(need);
      }
    } catch (const std::exception& e) {
      Logger::instance().print(LogLevel::Error,
                               "cache maintenance failed: ", e.what());
    }
    budget_->finishPass(sequence);
  }
}
