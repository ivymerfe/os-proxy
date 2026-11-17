#pragma once

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace platform {

inline bool setNonBlockCloexec(int fd) noexcept {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return false;
  }
  const int fdFlags = ::fcntl(fd, F_GETFD, 0);
  return fdFlags >= 0 && ::fcntl(fd, F_SETFD, fdFlags | FD_CLOEXEC) >= 0;
}

inline int prepare(int fd) noexcept {
  if (setNonBlockCloexec(fd)) {
    return fd;
  }
  const int e = errno;
  ::close(fd);
  errno = e;
  return -1;
}

inline int makeSocket(int domain, int type, int protocol) noexcept {
  const int fd = ::socket(domain, type, protocol);
  return fd < 0 ? -1 : prepare(fd);
}

inline int acceptConnection(int listener) noexcept {
  const int fd = ::accept(listener, nullptr, nullptr);
  return fd < 0 ? -1 : prepare(fd);
}

}  // namespace platform
