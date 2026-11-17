#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "CacheBudget.hpp"

struct FetchCancelled {};

class EntryReader;

class CacheEntry : public std::enable_shared_from_this<CacheEntry> {
 public:
  using Clock = std::chrono::steady_clock;
  enum class State { Loading, Complete, Failed };
  enum class ReadStatus { Data, End, Failed };

  CacheEntry(std::string key, std::shared_ptr<CacheBudget> budget);
  ~CacheEntry();
  CacheEntry(const CacheEntry&) = delete;
  CacheEntry& operator=(const CacheEntry&) = delete;

  const std::string& key() const noexcept { return key_; }
  std::unique_ptr<EntryReader> openReader();

  void setHead(std::string head);
  bool tryReserve(std::size_t bytes);
  void markUncacheable();
  bool cacheable() const noexcept { return cacheable_.load(); }
  bool append(const char* data, std::size_t size);
  void complete();
  void fail(int status, std::string message);

  bool expired(Clock::time_point now, Clock::duration ttl) const;
  bool evictable() const;
  std::size_t size() const;

 private:
  friend class EntryReader;

  std::string waitHead();
  ReadStatus read(const EntryReader& reader,
                  std::string& out,
                  std::size_t maxBytes);
  std::string failureMessage() const;
  void closeReader(const EntryReader& reader) noexcept;
  std::size_t minPositionLocked() const;

  const std::string key_;
  std::shared_ptr<CacheBudget> budget_;
  std::size_t reserved_{0};
  std::atomic<bool> cacheable_{true};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  State state_{State::Loading};
  std::string head_;
  bool headReady_{false};
  std::string body_;
  std::size_t base_{0};
  std::unordered_map<const EntryReader*, std::size_t> readers_;
  bool cancelled_{false};
  int failStatus_{500};
  std::string failMessage_;
  Clock::time_point completedAt_{};
};

class EntryReader {
 public:
  ~EntryReader() { entry_->closeReader(*this); }
  EntryReader(const EntryReader&) = delete;
  EntryReader& operator=(const EntryReader&) = delete;

  std::string waitHead() { return entry_->waitHead(); }
  CacheEntry::ReadStatus next(std::string& out, std::size_t maxBytes) {
    return entry_->read(*this, out, maxBytes);
  }
  std::string failureMessage() const { return entry_->failureMessage(); }

 private:
  friend class CacheEntry;
  explicit EntryReader(std::shared_ptr<CacheEntry> entry)
      : entry_(std::move(entry)) {}

  std::shared_ptr<CacheEntry> entry_;
};
