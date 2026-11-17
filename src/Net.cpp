#include "Net.hpp"

#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>

WaitResult Socket::wait(short events, std::chrono::milliseconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    pollfd fds[2] = {{fd_.get(), events, 0}, {stop_->fd(), POLLIN, 0}};
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());
    const int ms = static_cast<int>(
        std::max<long long>(0LL, static_cast<long long>(left.count())));
    const int rc = ::poll(fds, 2, ms);
    if (rc < 0) {
      const int e = errno;
      if (e == EINTR) {
        continue;
      }
      throw IoFailure(describeErrno("poll", e), e);
    }
    if ((fds[1].revents & POLLIN) != 0) {
      return WaitResult::Stopped;
    }
    if (rc == 0) {
      return WaitResult::Timeout;
    }
    if (fds[0].revents != 0) {
      return WaitResult::Ready;
    }
  }
}

std::size_t Socket::readSome(char* buffer,
                             std::size_t size,
                             std::chrono::milliseconds timeout) {
  for (;;) {
    const ssize_t n = ::recv(fd_.get(), buffer, size, 0);
    if (n >= 0) {
      return static_cast<std::size_t>(n);
    }
    const int e = errno;
    if (e == EINTR) {
      continue;
    }
    if (e != EAGAIN && e != EWOULDBLOCK) {
      throw IoFailure(describeErrno("recv", e), e);
    }
    switch (wait(POLLIN, timeout)) {
      case WaitResult::Ready:
        break;
      case WaitResult::Timeout:
        throw IoTimeout("read timed out");
      case WaitResult::Stopped:
        throw IoStopped("proxy is shutting down");
    }
  }
}

void Socket::writeAll(std::string_view data,
                      std::chrono::milliseconds timeout) {
  while (!data.empty()) {
    const ssize_t n = ::send(fd_.get(), data.data(), data.size(), MSG_NOSIGNAL);
    if (n >= 0) {
      data.remove_prefix(static_cast<std::size_t>(n));
      continue;
    }
    const int e = errno;
    if (e == EINTR) {
      continue;
    }
    if (e != EAGAIN && e != EWOULDBLOCK) {
      throw IoFailure(describeErrno("send", e), e);
    }
    switch (wait(POLLOUT, timeout)) {
      case WaitResult::Ready:
        break;
      case WaitResult::Timeout:
        throw IoTimeout("write timed out");
      case WaitResult::Stopped:
        throw IoStopped("proxy is shutting down");
    }
  }
}

void Socket::abort() noexcept {
  if (!fd_.valid()) {
    return;
  }
  linger option{1, 0};
  if (::setsockopt(fd_.get(), SOL_SOCKET, SO_LINGER, &option, sizeof option) <
      0) {
    Logger::instance().print(LogLevel::Debug,
                             describeErrno("setsockopt(SO_LINGER)", errno),
                             " fd=", fd_.get());
  }
  fd_.reset();
}
