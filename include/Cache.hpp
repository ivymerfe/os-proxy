#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

struct FetchCancelled {};

class Cache;
class EntryReader;

class CacheEntry : public std::enable_shared_from_this<CacheEntry> {
 public:
  using Clock = std::chrono::steady_clock;
  enum class State { Loading, Complete, Failed };
  enum class HeadStatus { Pending, Ready };
  enum class ReadStatus { Data, Pending, End, Failed };

  CacheEntry(std::string key, Cache& cache);
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

  struct ReaderState {
    std::size_t position{0};
    std::function<void()> notify;
  };

  HeadStatus tryHead(std::string& out);
  ReadStatus tryNext(const EntryReader& reader,
                     std::string& out,
                     std::size_t maxBytes);
  void setNotifier(const EntryReader& reader, std::function<void()> notify);
  std::string failureMessage() const;
  void closeReader(const EntryReader& reader) noexcept;
  std::size_t minPositionLocked() const;
  void notifyLocked();

  const std::string key_;
  Cache& cache_;
  std::size_t reserved_{0};
  std::atomic<bool> cacheable_{true};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  State state_{State::Loading};
  std::string head_;
  bool headReady_{false};
  std::string body_;
  std::size_t base_{0};
  std::unordered_map<const EntryReader*, ReaderState> readers_;
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

  CacheEntry::HeadStatus tryHead(std::string& out) {
    return entry_->tryHead(out);
  }
  CacheEntry::ReadStatus tryNext(std::string& out, std::size_t maxBytes) {
    return entry_->tryNext(*this, out, maxBytes);
  }
  void setNotifier(std::function<void()> notify) {
    entry_->setNotifier(*this, std::move(notify));
  }
  std::string failureMessage() const { return entry_->failureMessage(); }

 private:
  friend class CacheEntry;
  explicit EntryReader(std::shared_ptr<CacheEntry> entry)
      : entry_(std::move(entry)) {}

  std::shared_ptr<CacheEntry> entry_;
};

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
  Acquired acquireTransient(const std::string& key);
  void remove(const std::shared_ptr<CacheEntry>& entry);
  bool reserve(std::size_t bytes);
  void release(std::size_t bytes) noexcept;
  std::size_t maxEntrySize() const noexcept { return max_; }
  void shutdown();

 private:
  struct Slot {
    std::shared_ptr<CacheEntry> entry;
    std::list<std::string>::iterator lru;
  };

  void maintenanceLoop();
  void sweepExpired();
  void evict(std::size_t bytes);

  std::chrono::seconds ttl_;
  std::size_t capacity_;
  const std::size_t max_;
  std::size_t used_{0};
  std::uint64_t requested_{0};
  std::uint64_t doneSeq_{0};
  bool stopped_{false};
  std::mutex budgetMutex_;
  std::condition_variable wake_;
  std::condition_variable done_;

  std::mutex mutex_;
  std::unordered_map<std::string, Slot> map_;
  std::list<std::string> lru_;
  std::thread maintainer_;
};
