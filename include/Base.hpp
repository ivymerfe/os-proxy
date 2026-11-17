#pragma once

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "Logger.hpp"

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

class HttpError : public std::runtime_error {
 public:
  HttpError(int status, const std::string& message, int socketId = -1)
      : std::runtime_error(message), status_(status), socketId_(socketId) {}

  int status() const noexcept { return status_; }
  int socketId() const noexcept { return socketId_; }

 private:
  int status_;
  int socketId_;
};

struct IoTimeout : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct IoStopped : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct IoFailure : std::runtime_error {
  IoFailure(const std::string& message, int code)
      : std::runtime_error(message), code_(code) {}
  int code() const noexcept { return code_; }

 private:
  int code_;
};

inline std::string describeErrno(std::string_view what, int err) {
  std::string result(what);
  result += ": ";
  result += std::system_category().message(err);
  return result;
}

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

class Platform {
 public:
  static bool setNonBlockCloexec(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      return false;
    }
    const int fdFlags = ::fcntl(fd, F_GETFD, 0);
    return fdFlags >= 0 && ::fcntl(fd, F_SETFD, fdFlags | FD_CLOEXEC) >= 0;
  }

  static int makeSocket(int domain, int type, int protocol) noexcept {
    const int fd = ::socket(domain, type, protocol);
    return fd < 0 ? -1 : prepareSocket(fd);
  }

  static int acceptConnection(int listener) noexcept {
    const int fd = ::accept(listener, nullptr, nullptr);
    return fd < 0 ? -1 : prepareSocket(fd);
  }

 private:
  static int prepareSocket(int fd) noexcept {
    bool ok = setNonBlockCloexec(fd);
#ifdef SO_NOSIGPIPE
    const int one = 1;
    ok =
        ok && ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one) == 0;
#endif
    if (ok) {
      return fd;
    }
    const int e = errno;
    ::close(fd);
    errno = e;
    return -1;
  }
};
