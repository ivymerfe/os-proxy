#pragma once

#include <unistd.h>

#include <cerrno>
#include <utility>

#include "Errors.hpp"
#include "Logger.hpp"

class UniqueFd {
 public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
  UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      reset(other.release());
    }
    return *this;
  }
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  ~UniqueFd() { reset(); }

  int get() const noexcept { return fd_; }
  bool valid() const noexcept { return fd_ >= 0; }
  int release() noexcept { return std::exchange(fd_, -1); }

  void reset(int fd = -1) noexcept {
    const int old = std::exchange(fd_, fd);
    if (old >= 0 && ::close(old) < 0) {
      const int e = errno;
      if (e != EINTR) {
        Logger::instance().print(LogLevel::Warn, describeErrno("close", e),
                                 " fd=", old);
      }
    }
  }

 private:
  int fd_{-1};
};
