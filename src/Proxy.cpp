#include "Proxy.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <climits>
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
              const BodySource& body,
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
        request, body,
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

Session::Session(Socket socket,
                 Cache& cache,
                 ThreadPool& fetchPool,
                 const StopSignal& stop,
                 const Config& config,
                 std::function<void()> notify)
    : socket_(std::move(socket)),
      parser_(socket_.id()),
      cache_(cache),
      fetchPool_(fetchPool),
      stop_(stop),
      config_(config),
      notify_(std::move(notify)),
      deadline_(Clock::now() + config.clientTimeout) {}

short Session::events() const noexcept {
  switch (state_) {
    case State::Streaming:
      return outPos_ < out_.size() ? POLLOUT : POLLIN;
    case State::Flushing:
      return POLLOUT;
    case State::Done:
      return 0;
    default:
      return POLLIN;
  }
}

void Session::touch() {
  deadline_ = Clock::now() + config_.clientTimeout;
}

void Session::finish() noexcept {
  state_ = State::Done;
  reader_.reset();
}

void Session::send(std::string data) {
  out_ = std::move(data);
  outPos_ = 0;
  touch();
}

void Session::fail(int status, std::string_view message) {
  reader_.reset();
  if (headSent_) {
    socket_.abort();
    state_ = State::Done;
    return;
  }
  try {
    send(HttpResponses::error(status, message));
    state_ = State::Flushing;
  } catch (...) {
    state_ = State::Done;
  }
}

void Session::report() {
  Logger& log = Logger::instance();
  const int id = socket_.id();
  try {
    throw;
  } catch (const HttpError& e) {
    log.write(levelFor(e.status()), e.what(),
              e.socketId() >= 0 ? e.socketId() : id, url_);
    fail(e.status(), e.what());
  } catch (const IoFailure& e) {
    log.write(LogLevel::Warn, std::string("I/O failure: ") + e.what(), id,
              url_);
    finish();
  } catch (const std::bad_alloc&) {
    log.write(LogLevel::Error, "out of memory", id, url_);
    fail(500, "out of memory");
  } catch (const std::system_error& e) {
    log.write(LogLevel::Error, std::string("system error: ") + e.what(), id,
              url_);
    fail(500, "internal server error");
  } catch (const std::exception& e) {
    log.write(LogLevel::Error, std::string("internal error: ") + e.what(), id,
              url_);
    fail(500, "internal server error");
  } catch (...) {
    log.write(LogLevel::Error, "unknown internal error", id, url_);
    fail(500, "internal server error");
  }
}

void Session::advance() {
  for (int attempt = 0; attempt < 2 && state_ != State::Done; ++attempt) {
    try {
      while (state_ != State::Done && step()) {
      }
      return;
    } catch (...) {
      report();
    }
  }
}

void Session::onTimeout() {
  Logger::instance().write(LogLevel::Warn, "client timed out", socket_.id(),
                           url_);
  if (state_ == State::ReadingRequest || state_ == State::ReadingBody) {
    fail(408, "client timed out");
    return;
  }
  socket_.abort();
  finish();
}

bool Session::step() {
  switch (state_) {
    case State::ReadingRequest:
      return readRequest();
    case State::ReadingBody:
      return readBody();
    case State::WaitingHead:
      return waitHead();
    case State::Streaming:
      return stream();
    case State::Flushing:
      return flush();
    case State::Done:
      return false;
  }
  return false;
}

bool Session::readRequest() {
  char chunk[4096];
  for (;;) {
    const IoResult result = socket_.tryRead(chunk, sizeof chunk);
    if (result.status == IoStatus::WouldBlock) {
      return false;
    }
    if (result.status == IoStatus::Closed) {
      if (!received_) {
        Logger::instance().print(
            LogLevel::Debug,
            "client closed connection without request, socket=", socket_.id());
        finish();
        return false;
      }
      throw HttpError(400, "connection closed before the request was complete",
                      socket_.id());
    }
    received_ = true;
    touch();
    if (!parser_.feed(chunk, result.bytes)) {
      continue;
    }
    request_ = parser_.take();
    url_ = request_->url;
    Logger::instance().print(LogLevel::Log, request_->method, " ", url_,
                             " socket=", socket_.id());
    if (request_->contentLength.value_or(0) > kMaxRequestBody) {
      throw HttpError(413, "request body is too large", socket_.id());
    }
    if (request_->method == "GET") {
      startGet();
    } else {
      body_ = std::move(request_->bodyPrefix);
      state_ = State::ReadingBody;
    }
    return true;
  }
}

bool Session::readBody() {
  const std::size_t total = request_->contentLength.value_or(0);
  char chunk[16384];
  while (body_.size() < total) {
    const IoResult result =
        socket_.tryRead(chunk, std::min(sizeof chunk, total - body_.size()));
    if (result.status == IoStatus::WouldBlock) {
      return false;
    }
    if (result.status == IoStatus::Closed) {
      throw HttpError(
          400,
          "client closed the connection before the request body was complete",
          socket_.id());
    }
    body_.append(chunk, result.bytes);
    touch();
  }
  startDirect();
  return true;
}

void Session::startFetch(BodySource body,
                         const std::shared_ptr<CacheEntry>& entry) {
  auto copy = std::make_shared<const HttpRequest>(*request_);
  auto source = std::make_shared<BodySource>(std::move(body));
  Cache* cache = &cache_;
  const StopSignal* stop = &stop_;
  const Config* config = &config_;
  const bool scheduled =
      fetchPool_.trySubmit([cache, stop, config, entry, copy, source] {
        runFetch(*cache, *stop, *config, *copy, *source, entry);
      });
  if (!scheduled) {
    Logger::instance().write(LogLevel::Error,
                             "failed to schedule upstream fetch", socket_.id(),
                             url_);
    entry->fail(500, "failed to schedule upstream fetch");
    cache_.remove(entry);
  }
}

void Session::startGet() {
  auto acquired = cache_.acquire(url_);
  reader_ = std::move(acquired.reader);
  reader_->setNotifier(notify_);
  if (acquired.created) {
    Logger::instance().print(LogLevel::Debug, "cache miss: ", url_);
    startFetch({}, acquired.entry);
  } else {
    Logger::instance().print(LogLevel::Debug, "cache hit: ", url_);
  }
  state_ = State::WaitingHead;
  deadline_ = Clock::time_point::max();
}

void Session::startDirect() {
  auto acquired = cache_.acquireTransient(url_);
  reader_ = std::move(acquired.reader);
  reader_->setNotifier(notify_);
  auto body = std::make_shared<std::string>(std::move(body_));
  BodySource source;
  if (!body->empty()) {
    source = [body, finished = false](std::string& out) mutable {
      if (finished) {
        return false;
      }
      finished = true;
      out = std::move(*body);
      return true;
    };
  }
  startFetch(std::move(source), acquired.entry);
  state_ = State::WaitingHead;
  deadline_ = Clock::time_point::max();
}

void Session::checkPeer() {
  char scratch[512];
  for (;;) {
    const IoResult result = socket_.tryRead(scratch, sizeof scratch);
    if (result.status == IoStatus::Ok) {
      continue;
    }
    if (result.status == IoStatus::Closed) {
      Logger::instance().write(LogLevel::Debug, "client disconnected",
                               socket_.id(), url_);
      finish();
    }
    return;
  }
}

bool Session::waitHead() {
  std::string head;
  if (reader_->tryHead(head) == CacheEntry::HeadStatus::Pending) {
    checkPeer();
    return false;
  }
  send(std::move(head));
  headSent_ = true;
  state_ = State::Streaming;
  return true;
}

bool Session::stream() {
  for (;;) {
    while (outPos_ < out_.size()) {
      const IoResult result =
          socket_.tryWrite(out_.data() + outPos_, out_.size() - outPos_);
      if (result.status == IoStatus::WouldBlock) {
        return false;
      }
      outPos_ += result.bytes;
      touch();
    }
    switch (reader_->tryNext(out_, 64 * 1024)) {
      case CacheEntry::ReadStatus::Data:
        outPos_ = 0;
        break;
      case CacheEntry::ReadStatus::Pending:
        deadline_ = Clock::time_point::max();
        checkPeer();
        return false;
      case CacheEntry::ReadStatus::End:
        finish();
        return false;
      case CacheEntry::ReadStatus::Failed:
        throw HttpError(502, reader_->failureMessage(), socket_.id());
    }
  }
}

bool Session::flush() {
  while (outPos_ < out_.size()) {
    const IoResult result =
        socket_.tryWrite(out_.data() + outPos_, out_.size() - outPos_);
    if (result.status == IoStatus::WouldBlock) {
      return false;
    }
    outPos_ += result.bytes;
    touch();
  }
  finish();
  return false;
}

Reactor::Reactor(Cache& cache,
                 ThreadPool& fetchPool,
                 const StopSignal& stop,
                 const Config& config)
    : cache_(cache), fetchPool_(fetchPool), stop_(stop), config_(config) {
  int fds[2];
  if (::pipe(fds) < 0) {
    throw std::system_error(errno, std::generic_category(), "pipe");
  }
  wakeRead_.reset(fds[0]);
  wakeWrite_.reset(fds[1]);
  if (!Platform::setNonBlockCloexec(fds[0]) ||
      !Platform::setNonBlockCloexec(fds[1])) {
    throw std::system_error(errno, std::generic_category(), "fcntl");
  }
}

Reactor::~Reactor() {
  stop();
}

void Reactor::start() {
  thread_ = std::thread([this] { loop(); });
}

void Reactor::stop() {
  quit_.store(true);
  signal();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Reactor::signal() noexcept {
  const char one = 1;
  const ssize_t written = ::write(wakeWrite_.get(), &one, 1);
  (void)written;
}

bool Reactor::adopt(UniqueFd& fd) {
  {
    std::lock_guard lock(mutex_);
    if (!alive_.load()) {
      return false;
    }
    try {
      incoming_.push_back(std::move(fd));
    } catch (...) {
      return false;
    }
    load_.fetch_add(1);
  }
  signal();
  return true;
}

void Reactor::wake(std::uint64_t id) noexcept {
  try {
    std::lock_guard lock(mutex_);
    ready_.insert(id);
  } catch (...) {
  }
  signal();
}

void Reactor::drainWakeups() {
  char buffer[256];
  for (;;) {
    const ssize_t n = ::read(wakeRead_.get(), buffer, sizeof buffer);
    if (n > 0) {
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    return;
  }
}

void Reactor::adoptNow(UniqueFd&& fd) {
  const std::uint64_t id = nextId_++;
  try {
    auto session = std::make_unique<Session>(Socket(std::move(fd), stop_),
                                             cache_, fetchPool_, stop_, config_,
                                             [this, id] { wake(id); });
    Session* raw = session.get();
    sessions_.emplace(id, std::move(session));
    raw->advance();
  } catch (const std::exception& e) {
    Logger::instance().print(LogLevel::Error,
                             "failed to start session: ", e.what());
    sessions_.erase(id);
    load_.fetch_sub(1);
  }
}

void Reactor::loop() {
  Logger& log = Logger::instance();
  std::vector<pollfd> fds;
  std::vector<std::uint64_t> ids;
  while (!stop_.requested() && !quit_.load()) {
    fds.clear();
    ids.clear();
    fds.push_back({wakeRead_.get(), POLLIN, 0});
    fds.push_back({stop_.fd(), POLLIN, 0});
    auto nearest = Session::Clock::time_point::max();
    for (const auto& [id, session] : sessions_) {
      fds.push_back({session->fd(), session->events(), 0});
      ids.push_back(id);
      nearest = std::min(nearest, session->deadline());
    }
    int timeout = -1;
    if (nearest != Session::Clock::time_point::max()) {
      const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                            nearest - Session::Clock::now()) +
                        std::chrono::milliseconds(1);
      timeout =
          static_cast<int>(std::clamp<long long>(left.count(), 0, INT_MAX));
    }
    const int rc = ::poll(fds.data(), fds.size(), timeout);
    if (rc < 0) {
      const int e = errno;
      if (e == EINTR) {
        continue;
      }
      log.print(LogLevel::Error, describeErrno("poll", e),
                " in reactor, stopping it");
      break;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      break;
    }
    if ((fds[0].revents & POLLIN) != 0) {
      drainWakeups();
    }

    std::deque<UniqueFd> fresh;
    std::unordered_set<std::uint64_t> run;
    {
      std::lock_guard lock(mutex_);
      fresh.swap(incoming_);
      run.swap(ready_);
    }
    for (std::size_t i = 0; i < ids.size(); ++i) {
      if (fds[i + 2].revents != 0) {
        run.insert(ids[i]);
      }
    }
    for (const std::uint64_t id : run) {
      if (const auto it = sessions_.find(id); it != sessions_.end()) {
        it->second->advance();
      }
    }
    for (auto& fd : fresh) {
      adoptNow(std::move(fd));
    }
    const auto now = Session::Clock::now();
    for (auto& entry : sessions_) {
      Session& session = *entry.second;
      if (!session.done() && session.deadline() <= now) {
        session.onTimeout();
        session.advance();
      }
    }
    for (auto it = sessions_.begin(); it != sessions_.end();) {
      if (it->second->done()) {
        it = sessions_.erase(it);
        load_.fetch_sub(1);
      } else {
        ++it;
      }
    }
  }
  alive_.store(false);
  sessions_.clear();
  std::lock_guard lock(mutex_);
  incoming_.clear();
  load_.store(0);
}

void Server::reject(UniqueFd fd, int status, std::string_view message) {
  try {
    Socket socket(std::move(fd), stop_);
    const std::string text = HttpResponses::error(status, message);
    socket.tryWrite(text.data(), text.size());
  } catch (const std::exception& e) {
    Logger::instance().print(LogLevel::Debug,
                             "failed to deliver rejection: ", e.what());
  }
}

std::size_t Server::totalLoad() const {
  std::size_t total = 0;
  for (const auto& reactor : reactors_) {
    total += reactor->load();
  }
  return total;
}

Reactor& Server::pickReactor() {
  return **std::min_element(
      reactors_.begin(), reactors_.end(),
      [](const auto& a, const auto& b) { return a->load() < b->load(); });
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
    reject(UniqueFd(fd), 503, "server is out of file descriptors");
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

  UniqueFd client(fd);
  if (totalLoad() >= config_.maxSessions) {
    Logger::instance().write(
        LogLevel::Warn, "session limit reached, rejecting connection", fd, "-");
    reject(std::move(client), 503,
           "server is overloaded, please try again later");
    return;
  }
  if (!pickReactor().adopt(client)) {
    Logger::instance().write(LogLevel::Error,
                             "no reactor accepted the connection", fd, "-");
    reject(std::move(client), 503, "server is unavailable");
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
        std::make_unique<ThreadPool>("fetch", config_.clientThreads * 2,
                                     std::numeric_limits<std::size_t>::max());
    for (std::size_t i = 0; i < config_.clientThreads; ++i) {
      reactors_.push_back(
          std::make_unique<Reactor>(*cache_, *fetchPool_, stop_, config_));
    }
    for (auto& reactor : reactors_) {
      reactor->start();
    }

    const int spare = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (spare < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "open(/dev/null)");
    }
    spare_.reset(spare);
    listener_ = createListener();

    log.print(LogLevel::Notice, "Proxy listening on port ", config_.port,
              ", client threads: ", config_.clientThreads,
              ", max sessions: ", config_.maxSessions,
              ", cache: ", config_.cacheInitialSize, "..", config_.cacheMaxSize,
              " bytes, ttl: ", config_.cacheTtl.count(), "s");

    acceptLoop();

    log.print(LogLevel::Notice, "Received shutdown signal");
    listener_.reset();
    log.print(LogLevel::Notice, "Stopped accepting new connections");
    for (auto& reactor : reactors_) {
      reactor->stop();
    }
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
