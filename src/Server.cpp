#include "Server.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <limits>
#include <memory>
#include <system_error>

#include "ClientSession.hpp"
#include "Errors.hpp"
#include "Http.hpp"
#include "Logger.hpp"
#include "Platform.hpp"

UniqueFd Server::createListener() {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  addrinfo* raw = nullptr;
  const int rc = ::getaddrinfo(nullptr, std::to_string(config_.port).c_str(),
                               &hints, &raw);
  if (rc != 0) {
    throw std::runtime_error(std::string("getaddrinfo failed: ") +
                             ::gai_strerror(rc));
  }
  std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> list(raw,
                                                            &::freeaddrinfo);

  std::string lastError = "no address to bind";
  for (const addrinfo* ai = list.get(); ai != nullptr; ai = ai->ai_next) {
    UniqueFd fd(
        platform::makeSocket(ai->ai_family, ai->ai_socktype, ai->ai_protocol));
    if (!fd.valid()) {
      lastError = describeErrno("socket", errno);
      continue;
    }
    const int enable = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &enable,
                     sizeof enable) < 0) {
      lastError = describeErrno("setsockopt(SO_REUSEADDR)", errno);
      continue;
    }
    if (ai->ai_family == AF_INET6) {
      const int disable = 0;
      if (::setsockopt(fd.get(), IPPROTO_IPV6, IPV6_V6ONLY, &disable,
                       sizeof disable) < 0) {
        lastError = describeErrno("setsockopt(IPV6_V6ONLY)", errno);
        continue;
      }
    }
    if (::bind(fd.get(), ai->ai_addr, ai->ai_addrlen) < 0) {
      lastError = describeErrno("bind", errno);
      continue;
    }
    if (::listen(fd.get(), SOMAXCONN) < 0) {
      lastError = describeErrno("listen", errno);
      continue;
    }
    return fd;
  }
  throw std::runtime_error("cannot create listening socket: " + lastError);
}

void Server::handleDescriptorExhaustion() {
  Logger::instance().print(LogLevel::Error,
                           "descriptor limit reached, rejecting connection");
  spare_.reset();
  const int fd = platform::acceptConnection(listener_.get());
  if (fd >= 0) {
    try {
      Socket socket(UniqueFd(fd), stop_);
      socket.writeAll(
          HttpResponses::error(503, "server is out of file descriptors"),
          std::chrono::seconds(1));
    } catch (...) {
    }
  }
  const int spare = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
  if (spare < 0) {
    Logger::instance().print(LogLevel::Error,
                             describeErrno("open(/dev/null)", errno));
  } else {
    spare_.reset(spare);
  }
}

void Server::acceptOne(Cache& cache,
                       Fetcher& fetcher,
                       ThreadPool& fetchPool,
                       ThreadPool& clientPool) {
  const int fd = platform::acceptConnection(listener_.get());
  if (fd < 0) {
    const int e = errno;
    if (e == EINTR || e == EAGAIN || e == EWOULDBLOCK || e == ECONNABORTED) {
      return;
    }
    if (e == EMFILE || e == ENFILE || e == ENOBUFS || e == ENOMEM) {
      handleDescriptorExhaustion();
      return;
    }
    Logger::instance().print(LogLevel::Error, describeErrno("accept", e));
    return;
  }

  auto session = std::make_shared<ClientSession>(
      Socket(UniqueFd(fd), stop_), cache, fetcher, fetchPool, stop_, config_);
  if (!clientPool.trySubmit([session] { session->run(); })) {
    Logger::instance().write(
        LogLevel::Warn, "client queue is full, rejecting connection", fd, "-");
    session->reject(503, "server is overloaded, please try again later");
  }
}

void Server::acceptLoop(Cache& cache,
                        Fetcher& fetcher,
                        ThreadPool& fetchPool,
                        ThreadPool& clientPool) {
  while (!stop_.requested()) {
    pollfd fds[2] = {{listener_.get(), POLLIN, 0}, {stop_.fd(), POLLIN, 0}};
    const int rc = ::poll(fds, 2, -1);
    if (rc < 0) {
      const int e = errno;
      if (e == EINTR) {
        continue;
      }
      throw std::system_error(e, std::generic_category(), "poll");
    }
    if ((fds[1].revents & POLLIN) != 0) {
      return;
    }
    if ((fds[0].revents & POLLIN) != 0) {
      try {
        acceptOne(cache, fetcher, fetchPool, clientPool);
      } catch (const std::exception& e) {
        Logger::instance().print(LogLevel::Error,
                                 "failed to accept connection: ", e.what());
      }
    }
  }
}

int Server::run() {
  Logger& log = Logger::instance();
  try {
    Cache cache(config_.cacheInitialSize, config_.cacheMaxSize,
                config_.cacheTtl);
    Fetcher fetcher(cache, stop_, config_.upstreamTimeout);
    ThreadPool fetchPool("server", config_.clientThreads,
                         std::numeric_limits<std::size_t>::max());
    ThreadPool clientPool("client", config_.clientThreads, config_.queueSize);

    const int spare = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (spare < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "open(/dev/null)");
    }
    spare_.reset(spare);
    listener_ = createListener();

    log.print(LogLevel::Notice, "Proxy listening on port ", config_.port,
              ", client threads: ", config_.clientThreads,
              ", queue: ", config_.queueSize,
              ", cache: ", config_.cacheInitialSize, "..", config_.cacheMaxSize,
              " bytes, ttl: ", config_.cacheTtl.count(), "s");

    acceptLoop(cache, fetcher, fetchPool, clientPool);

    log.print(LogLevel::Notice, "Received shutdown signal");
    listener_.reset();
    log.print(LogLevel::Notice, "Stopped accepting new connections");
    clientPool.shutdown(false);
    log.print(LogLevel::Notice, "All connections closed");
    cache.shutdown();
    fetchPool.shutdown(true);
    log.print(LogLevel::Notice, "All threads have finished");
  } catch (const std::exception& e) {
    log.print(LogLevel::Error, "fatal error: ", e.what());
    return 1;
  }
  log.print(LogLevel::Notice, "Proxy finished");
  return 0;
}
