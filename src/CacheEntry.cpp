#include "CacheEntry.hpp"

#include <algorithm>

#include "Errors.hpp"

namespace {
constexpr std::size_t kWindowBytes = 256 * 1024;
constexpr std::size_t kCompactStep = 64 * 1024;
}  // namespace

CacheEntry::CacheEntry(std::string key, std::shared_ptr<CacheBudget> budget)
    : key_(std::move(key)), budget_(std::move(budget)) {}

CacheEntry::~CacheEntry() {
  budget_->release(reserved_);
}

std::unique_ptr<EntryReader> CacheEntry::openReader() {
  std::unique_ptr<EntryReader> reader(new EntryReader(shared_from_this()));
  std::lock_guard lock(mutex_);
  if (cancelled_ || state_ == State::Failed || !cacheable_.load()) {
    return nullptr;
  }
  readers_.emplace(reader.get(), 0);
  return reader;
}

void CacheEntry::closeReader(const EntryReader& reader) noexcept {
  try {
    std::lock_guard lock(mutex_);
    if (readers_.erase(&reader) == 0) {
      return;
    }
    if (readers_.empty() && state_ == State::Loading) {
      cancelled_ = true;
    }
    cv_.notify_all();
  } catch (...) {
  }
}

void CacheEntry::setHead(std::string head) {
  {
    std::lock_guard lock(mutex_);
    head_ = std::move(head);
    headReady_ = true;
  }
  cv_.notify_all();
}

bool CacheEntry::tryReserve(std::size_t bytes) {
  if (!budget_->reserve(bytes)) {
    return false;
  }
  reserved_ += bytes;
  return true;
}

void CacheEntry::markUncacheable() {
  if (!cacheable_.exchange(false)) {
    return;
  }
  budget_->release(reserved_);
  reserved_ = 0;
  cv_.notify_all();
}

std::size_t CacheEntry::minPositionLocked() const {
  std::size_t position = base_ + body_.size();
  for (const auto& item : readers_) {
    position = std::min(position, item.second);
  }
  return position;
}

bool CacheEntry::append(const char* data, std::size_t size) {
  std::unique_lock lock(mutex_);
  if (!cacheable_.load()) {
    cv_.wait(lock, [&] {
      return cancelled_ ||
             base_ + body_.size() - minPositionLocked() < kWindowBytes;
    });
  }
  if (cancelled_) {
    return false;
  }
  body_.append(data, size);
  cv_.notify_all();
  return true;
}

void CacheEntry::complete() {
  {
    std::lock_guard lock(mutex_);
    if (state_ != State::Loading) {
      return;
    }
    state_ = State::Complete;
    completedAt_ = Clock::now();
  }
  cv_.notify_all();
}

void CacheEntry::fail(int status, std::string message) {
  {
    std::lock_guard lock(mutex_);
    if (state_ != State::Loading) {
      return;
    }
    state_ = State::Failed;
    failStatus_ = status;
    failMessage_ = std::move(message);
  }
  cv_.notify_all();
}

std::string CacheEntry::waitHead() {
  std::unique_lock lock(mutex_);
  cv_.wait(lock, [&] { return headReady_ || state_ == State::Failed; });
  if (!headReady_) {
    throw HttpError(failStatus_, failMessage_);
  }
  return head_;
}

CacheEntry::ReadStatus CacheEntry::read(const EntryReader& reader,
                                        std::string& out,
                                        std::size_t maxBytes) {
  std::unique_lock lock(mutex_);
  const auto it = readers_.find(&reader);
  if (it == readers_.end()) {
    return ReadStatus::Failed;
  }
  std::size_t& position = it->second;
  cv_.wait(lock, [&] {
    return base_ + body_.size() > position || state_ != State::Loading;
  });
  const std::size_t total = base_ + body_.size();
  if (total > position) {
    const std::size_t n = std::min(maxBytes, total - position);
    out.assign(body_, position - base_, n);
    position += n;
    if (!cacheable_.load()) {
      const std::size_t minimum = minPositionLocked();
      if (minimum - base_ >= kCompactStep) {
        body_.erase(0, minimum - base_);
        base_ = minimum;
      }
      cv_.notify_all();
    }
    return ReadStatus::Data;
  }
  return state_ == State::Complete ? ReadStatus::End : ReadStatus::Failed;
}

std::string CacheEntry::failureMessage() const {
  std::lock_guard lock(mutex_);
  return failMessage_.empty() ? "upstream transfer failed" : failMessage_;
}

bool CacheEntry::expired(Clock::time_point now, Clock::duration ttl) const {
  std::lock_guard lock(mutex_);
  return state_ == State::Complete && now - completedAt_ >= ttl;
}

bool CacheEntry::evictable() const {
  std::lock_guard lock(mutex_);
  return state_ == State::Complete && readers_.empty();
}

std::size_t CacheEntry::size() const {
  std::lock_guard lock(mutex_);
  return body_.size();
}
