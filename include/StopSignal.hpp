#pragma once

#include <atomic>

#include "UniqueFd.hpp"

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
