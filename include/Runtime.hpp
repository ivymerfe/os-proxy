#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "Base.hpp"

class StopSignal {
 public:
  StopSignal();

  void request() noexcept;
  bool requested() const noexcept {
    return flag_.load(std::memory_order_relaxed);
  }
  int fd() const noexcept { return readEnd_.get(); }

 private:
  UniqueFd readEnd_;
  UniqueFd writeEnd_;
  std::atomic<bool> flag_{false};
};

class SignalHandler {
 public:
  static void install(StopSignal& stop);

 private:
  static void handle(int) noexcept;
  static std::atomic<StopSignal*> target_;
};

class ThreadPool {
 public:
  ThreadPool(std::string name, std::size_t threads, std::size_t maxQueue);
  ~ThreadPool();
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  bool trySubmit(std::function<void()> task) noexcept;
  void shutdown(bool drain);

 private:
  void workerLoop();

  std::string name_;
  std::size_t maxQueue_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  bool stopping_{false};
};
