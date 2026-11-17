#include "Runtime.hpp"

#include <csignal>
#include <system_error>

StopSignal::StopSignal() {
  int fds[2];
  if (::pipe(fds) < 0) {
    throw std::system_error(errno, std::generic_category(), "pipe");
  }
  UniqueFd readEnd(fds[0]);
  UniqueFd writeEnd(fds[1]);
  if (!Platform::setNonBlockCloexec(fds[0]) ||
      !Platform::setNonBlockCloexec(fds[1])) {
    throw std::system_error(errno, std::generic_category(), "fcntl");
  }
  readEnd_ = std::move(readEnd);
  writeEnd_ = std::move(writeEnd);
}

void StopSignal::request() noexcept {
  flag_.store(true);
  const char one = 1;
  const ssize_t written = ::write(writeEnd_.get(), &one, 1);
  (void)written;
}

std::atomic<StopSignal*> SignalHandler::target_{nullptr};

void SignalHandler::handle(int) noexcept {
  const int saved = errno;
  if (StopSignal* stop = target_.load()) {
    stop->request();
  }
  errno = saved;
}

void SignalHandler::install(StopSignal& stop) {
  target_.store(&stop);

  struct sigaction action{};
  action.sa_handler = &SignalHandler::handle;
  sigemptyset(&action.sa_mask);
  for (const int sig : {SIGINT, SIGTERM}) {
    if (::sigaction(sig, &action, nullptr) < 0) {
      throw std::system_error(errno, std::generic_category(), "sigaction");
    }
  }

  struct sigaction ignore{};
  ignore.sa_handler = SIG_IGN;
  sigemptyset(&ignore.sa_mask);
  if (::sigaction(SIGPIPE, &ignore, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "sigaction(SIGPIPE)");
  }
}

ThreadPool::ThreadPool(std::string name,
                       std::size_t threads,
                       std::size_t maxQueue)
    : name_(std::move(name)), maxQueue_(maxQueue) {
  workers_.reserve(threads);
  try {
    for (std::size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] { workerLoop(); });
    }
  } catch (...) {
    shutdown(false);
    throw;
  }
}

ThreadPool::~ThreadPool() {
  shutdown(false);
}

bool ThreadPool::trySubmit(std::function<void()> task) noexcept {
  try {
    std::lock_guard lock(mutex_);
    if (stopping_ || queue_.size() >= maxQueue_) {
      return false;
    }
    queue_.push_back(std::move(task));
  } catch (...) {
    return false;
  }
  cv_.notify_one();
  return true;
}

void ThreadPool::shutdown(bool drain) {
  std::size_t dropped = 0;
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
    if (!drain) {
      dropped = queue_.size();
      queue_.clear();
    }
  }
  cv_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  if (dropped != 0) {
    Logger::instance().print(LogLevel::Notice, "pool '", name_, "': dropped ",
                             dropped, " pending tasks");
  }
}

void ThreadPool::workerLoop() {
  for (;;) {
    std::function<void()> task;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) {
        return;
      }
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    try {
      task();
    } catch (const std::exception& e) {
      Logger::instance().print(LogLevel::Error, "pool '", name_,
                               "': task failed: ", e.what());
    } catch (...) {
      Logger::instance().print(LogLevel::Error, "pool '", name_,
                               "': task failed with unknown exception");
    }
  }
}
