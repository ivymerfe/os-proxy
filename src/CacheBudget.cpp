#include "CacheBudget.hpp"

#include <algorithm>

CacheBudget::CacheBudget(std::size_t initial, std::size_t max)
    : capacity_(std::min(initial, max)), max_(max) {}

bool CacheBudget::reserve(std::size_t bytes) {
  std::unique_lock lock(mutex_);
  if (stopped_ || bytes > max_) {
    return false;
  }
  if (used_ + bytes <= capacity_) {
    used_ += bytes;
    return true;
  }
  const std::uint64_t sequence = ++requested_;
  wake_.notify_one();
  done_.wait(lock, [&] { return stopped_ || doneSeq_ >= sequence; });
  if (stopped_ || used_ + bytes > capacity_) {
    return false;
  }
  used_ += bytes;
  return true;
}

void CacheBudget::release(std::size_t bytes) noexcept {
  try {
    std::lock_guard lock(mutex_);
    used_ = bytes > used_ ? 0 : used_ - bytes;
  } catch (...) {
  }
}

bool CacheBudget::waitForWork(std::chrono::milliseconds period) {
  std::unique_lock lock(mutex_);
  wake_.wait_for(lock, period,
                 [&] { return stopped_ || requested_ > doneSeq_; });
  return !stopped_;
}

std::uint64_t CacheBudget::beginPass() {
  std::lock_guard lock(mutex_);
  if (requested_ > doneSeq_ && capacity_ < max_) {
    capacity_ = capacity_ > max_ / 2 ? max_ : capacity_ * 2;
  }
  return requested_;
}

std::size_t CacheBudget::bytesToEvict() const {
  std::lock_guard lock(mutex_);
  if (requested_ <= doneSeq_ || capacity_ < max_) {
    return 0;
  }
  const std::size_t target = capacity_ - capacity_ / 4;
  return used_ > target ? used_ - target : 0;
}

void CacheBudget::finishPass(std::uint64_t sequence) {
  {
    std::lock_guard lock(mutex_);
    doneSeq_ = std::max(doneSeq_, sequence);
  }
  done_.notify_all();
}

void CacheBudget::stop() {
  {
    std::lock_guard lock(mutex_);
    stopped_ = true;
  }
  wake_.notify_all();
  done_.notify_all();
}
