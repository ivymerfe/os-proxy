#include "Cache.hpp"

#include <algorithm>

#include "Base.hpp"

namespace {
constexpr std::size_t kWindowBytes = 256 * 1024;
constexpr std::size_t kCompactStep = 64 * 1024;
}  // namespace

CacheEntry::CacheEntry(std::string key, Cache& cache)
    : key_(std::move(key)), cache_(cache) {}

CacheEntry::~CacheEntry() {
  cache_.release(reserved_);
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
  if (reserved_ + bytes > cache_.maxEntrySize() || !cache_.reserve(bytes)) {
    return false;
  }
  reserved_ += bytes;
  return true;
}

void CacheEntry::markUncacheable() {
  if (!cacheable_.exchange(false)) {
    return;
  }
  cache_.release(reserved_);
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

Cache::Cache(std::size_t initialSize,
             std::size_t maxSize,
             std::chrono::seconds ttl)
    : ttl_(ttl), capacity_(std::min(initialSize, maxSize)), max_(maxSize) {
  maintainer_ = std::thread([this] { maintenanceLoop(); });
}

Cache::~Cache() {
  shutdown();
}

void Cache::shutdown() {
  {
    std::lock_guard lock(budgetMutex_);
    stopped_ = true;
  }
  wake_.notify_all();
  done_.notify_all();
  if (maintainer_.joinable()) {
    maintainer_.join();
  }
  std::lock_guard lock(mutex_);
  map_.clear();
  lru_.clear();
}

bool Cache::reserve(std::size_t bytes) {
  std::unique_lock lock(budgetMutex_);
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

void Cache::release(std::size_t bytes) noexcept {
  try {
    std::lock_guard lock(budgetMutex_);
    used_ = bytes > used_ ? 0 : used_ - bytes;
  } catch (...) {
  }
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

  auto entry = std::make_shared<CacheEntry>(key, *this);
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
  for (;;) {
    std::uint64_t sequence = 0;
    std::size_t toEvict = 0;
    {
      std::unique_lock lock(budgetMutex_);
      wake_.wait_for(lock, period,
                     [&] { return stopped_ || requested_ > doneSeq_; });
      if (stopped_) {
        return;
      }
      sequence = requested_;
      if (requested_ > doneSeq_) {
        if (capacity_ < max_) {
          capacity_ = capacity_ > max_ / 2 ? max_ : capacity_ * 2;
        } else {
          const std::size_t target = capacity_ - capacity_ / 4;
          toEvict = used_ > target ? used_ - target : 0;
        }
      }
    }
    try {
      sweepExpired();
      if (toEvict != 0) {
        evict(toEvict);
      }
    } catch (const std::exception& e) {
      Logger::instance().print(LogLevel::Error,
                               "cache maintenance failed: ", e.what());
    }
    {
      std::lock_guard lock(budgetMutex_);
      doneSeq_ = std::max(doneSeq_, sequence);
    }
    done_.notify_all();
  }
}
