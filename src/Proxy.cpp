#include "Proxy.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <new>
#include <system_error>

namespace {

LogLevel levelFor(int status) {
  return status == 500 ? LogLevel::Error : LogLevel::Warn;
}

void runFetch(Cache& cache,
              const StopSignal& stop,
              const Config& config,
              const HttpRequest& request,
              const std::shared_ptr<CacheEntry>& entry) {
  int status = 500;
  int socketId = -1;
  LogLevel level = LogLevel::Error;
  std::string message;

  const auto detach = [&] {
    entry->markUncacheable();
    cache.remove(entry);
  };

  try {
    if (stop.requested()) {
      throw IoStopped("proxy is shutting down");
    }
    UpstreamClient client(stop, config.upstreamTimeout);
    client.exchange(
        request, {},
        [&](const ResponseHead& head) {
          const bool tooLarge =
              head.contentLength && *head.contentLength > cache.maxEntrySize();
          if (head.status != 200 || tooLarge) {
            detach();
          }
          entry->setHead(head.serializeForClient());
        },
        [&](const char* data, std::size_t size) {
          if (entry->cacheable() && !entry->tryReserve(size)) {
            detach();
          }
          if (!entry->append(data, size)) {
            throw FetchCancelled{};
          }
        });
    entry->complete();
    return;
  } catch (const HttpError& e) {
    status = e.status();
    socketId = e.socketId();
    message = e.what();
    level = status == 500 ? LogLevel::Error : LogLevel::Warn;
  } catch (const IoStopped&) {
    status = 503;
    message = "proxy is shutting down";
    level = LogLevel::Notice;
  } catch (const FetchCancelled&) {
    message = "transfer cancelled: no clients left";
    level = LogLevel::Debug;
  } catch (const IoFailure& e) {
    message = std::string("I/O failure while fetching resource: ") + e.what();
  } catch (const std::bad_alloc&) {
    message = "out of memory while fetching resource";
  } catch (const std::exception& e) {
    message =
        std::string("internal error while fetching resource: ") + e.what();
  } catch (...) {
    message = "unknown internal error while fetching resource";
  }
  Logger::instance().write(
      level,
      "upstream fetch failed (" + std::to_string(status) + "): " + message,
      socketId, request.url);
  entry->fail(status, message);
  cache.remove(entry);
}

}  // namespace

ClientSession::ClientSession(Socket socket,
                             Cache& cache,
                             ThreadPool& fetchPool,
                             const StopSignal& stop,
                             const Config& config)
    : socket_(std::move(socket)),
      cache_(cache),
      fetchPool_(fetchPool),
      stop_(stop),
      config_(config) {}

void ClientSession::run() {
  Logger& log = Logger::instance();
  const int id = socket_.id();
  try {
    handle();
    return;
  } catch (const HttpError& e) {
    log.write(levelFor(e.status()), e.what(),
              e.socketId() >= 0 ? e.socketId() : id, url_);
    respond(e.status(), e.what());
  } catch (const IoTimeout& e) {
    log.write(LogLevel::Warn, std::string("client timeout: ") + e.what(), id,
              url_);
    respond(408, "client timed out");
  } catch (const IoStopped&) {
    log.write(LogLevel::Notice, "connection closed: proxy is shutting down", id,
              url_);
  } catch (const IoFailure& e) {
    log.write(LogLevel::Warn, std::string("I/O failure: ") + e.what(), id,
              url_);
  } catch (const std::bad_alloc&) {
    log.write(LogLevel::Error, "out of memory", id, url_);
    respond(500, "out of memory");
  } catch (const std::system_error& e) {
    log.write(LogLevel::Error, std::string("system error: ") + e.what(), id,
              url_);
    respond(500, "internal server error");
  } catch (const std::exception& e) {
    log.write(LogLevel::Error, std::string("internal error: ") + e.what(), id,
              url_);
    respond(500, "internal server error");
  } catch (...) {
    log.write(LogLevel::Error, "unknown internal error", id, url_);
    respond(500, "internal server error");
  }
}

void ClientSession::respond(int status, std::string_view message) {
  if (headSent_) {
    socket_.abort();
    return;
  }
  sendError(status, message);
}

void ClientSession::reject(int status, std::string_view message) {
  sendError(status, message);
}

void ClientSession::sendError(int status, std::string_view message) {
  try {
    socket_.writeAll(HttpResponses::error(status, message),
                     std::chrono::seconds(5));
  } catch (const std::exception& e) {
    Logger::instance().write(
        LogLevel::Warn,
        std::string("failed to deliver error response: ") + e.what(),
        socket_.id(), url_);
  }
}

void ClientSession::handle() {
  RequestReader reader(socket_, config_.clientTimeout);
  const auto request = reader.read();
  if (!request) {
    Logger::instance().print(
        LogLevel::Debug,
        "client closed connection without request, socket=", socket_.id());
    return;
  }
  url_ = request->url;
  Logger::instance().print(LogLevel::Log, request->method, " ", request->url,
                           " socket=", socket_.id());
  if (request->method == "GET") {
    serveCached(*request);
  } else {
    serveDirect(*request);
  }
}

void ClientSession::startFetch(const HttpRequest& request,
                               const std::shared_ptr<CacheEntry>& entry) {
  auto copy = std::make_shared<const HttpRequest>(request);
  Cache* cache = &cache_;
  const StopSignal* stop = &stop_;
  const Config* config = &config_;
  const bool scheduled =
      fetchPool_.trySubmit([cache, stop, config, entry, copy] {
        runFetch(*cache, *stop, *config, *copy, entry);
      });
  if (!scheduled) {
    Logger::instance().write(LogLevel::Error,
                             "failed to schedule upstream fetch", socket_.id(),
                             request.url);
    entry->fail(500, "failed to schedule upstream fetch");
    cache_.remove(entry);
  }
}

void ClientSession::serveCached(const HttpRequest& request) {
  auto acquired = cache_.acquire(request.url);
  if (acquired.created) {
    Logger::instance().print(LogLevel::Debug, "cache miss: ", request.url);
    startFetch(request, acquired.entry);
  } else {
    Logger::instance().print(LogLevel::Debug, "cache hit: ", request.url);
  }

  const std::string head = acquired.reader->waitHead();
  headSent_ = true;
  socket_.writeAll(head, config_.clientTimeout);

  std::string chunk;
  for (;;) {
    switch (acquired.reader->next(chunk, 64 * 1024)) {
      case CacheEntry::ReadStatus::Data:
        socket_.writeAll(chunk, config_.clientTimeout);
        break;
      case CacheEntry::ReadStatus::End:
        return;
      case CacheEntry::ReadStatus::Failed:
        throw HttpError(502, acquired.reader->failureMessage(), socket_.id());
    }
  }
}

void ClientSession::serveDirect(const HttpRequest& request) {
  UpstreamClient client(stop_, config_.upstreamTimeout);
  std::string prefix = request.bodyPrefix;
  std::size_t remaining = request.contentLength.value_or(0);

  BodySource body;
  if (remaining > 0) {
    body = [&](std::string& out) -> bool {
      if (remaining == 0) {
        return false;
      }
      if (!prefix.empty()) {
        out = std::move(prefix);
        prefix.clear();
        remaining -= out.size();
        return true;
      }
      char buffer[16384];
      const std::size_t n = socket_.readSome(
          buffer, std::min(sizeof buffer, remaining), config_.clientTimeout);
      if (n == 0) {
        throw IoFailure(
            "client closed the connection before the request body was complete",
            0);
      }
      out.assign(buffer, n);
      remaining -= n;
      return true;
    };
  }

  client.exchange(
      request, body,
      [&](const ResponseHead& head) {
        const std::string text = head.serializeForClient();
        headSent_ = true;
        socket_.writeAll(text, config_.clientTimeout);
      },
      [&](const char* data, std::size_t size) {
        socket_.writeAll(std::string_view(data, size), config_.clientTimeout);
      });
}

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
        Platform::makeSocket(ai->ai_family, ai->ai_socktype, ai->ai_protocol));
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
  const int fd = Platform::acceptConnection(listener_.get());
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

void Server::acceptOne() {
  const int fd = Platform::acceptConnection(listener_.get());
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
      Socket(UniqueFd(fd), stop_), *cache_, *fetchPool_, stop_, config_);
  if (!clientPool_->trySubmit([session] { session->run(); })) {
    Logger::instance().write(
        LogLevel::Warn, "client queue is full, rejecting connection", fd, "-");
    session->reject(503, "server is overloaded, please try again later");
  }
}

void Server::acceptLoop() {
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
        acceptOne();
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
    cache_ = std::make_unique<Cache>(config_.cacheInitialSize,
                                     config_.cacheMaxSize, config_.cacheTtl);
    fetchPool_ =
        std::make_unique<ThreadPool>("fetch", config_.clientThreads,
                                     std::numeric_limits<std::size_t>::max());
    clientPool_ = std::make_unique<ThreadPool>("client", config_.clientThreads,
                                               config_.queueSize);

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

    acceptLoop();

    log.print(LogLevel::Notice, "Received shutdown signal");
    listener_.reset();
    log.print(LogLevel::Notice, "Stopped accepting new connections");
    clientPool_->shutdown(false);
    log.print(LogLevel::Notice, "All connections closed");
    cache_->shutdown();
    fetchPool_->shutdown(true);
    log.print(LogLevel::Notice, "All threads have finished");
  } catch (const std::exception& e) {
    log.print(LogLevel::Error, "fatal error: ", e.what());
    return 1;
  }
  log.print(LogLevel::Notice, "Proxy finished");
  return 0;
}
