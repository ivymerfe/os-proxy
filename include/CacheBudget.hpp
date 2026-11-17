#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>

class CacheBudget {
 public:
  CacheBudget(std::size_t initial, std::size_t max);

  bool reserve(std::size_t bytes);
  void release(std::size_t bytes) noexcept;
  std::size_t maxSize() const noexcept { return max_; }

  bool waitForWork(std::chrono::milliseconds period);
  std::uint64_t beginPass();
  std::size_t bytesToEvict() const;
  void finishPass(std::uint64_t sequence);
  void stop();

 private:
  mutable std::mutex mutex_;
  std::condition_variable wake_;
  std::condition_variable done_;
  std::size_t capacity_;
  const std::size_t max_;
  std::size_t used_{0};
  std::uint64_t requested_{0};
  std::uint64_t doneSeq_{0};
  bool stopped_{false};
};
