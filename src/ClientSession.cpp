#include "ClientSession.hpp"

#include <algorithm>
#include <memory>
#include <new>
#include <system_error>

#include "Errors.hpp"
#include "Logger.hpp"
#include "ResponseSink.hpp"
#include "UpstreamClient.hpp"

namespace {

LogLevel levelFor(int status) {
  return status == 500 ? LogLevel::Error : LogLevel::Warn;
}

class ClientBodySource final : public RequestBodySource {
 public:
  ClientBodySource(Socket& socket,
                   std::string prefix,
                   std::size_t total,
                   std::chrono::milliseconds timeout)
      : socket_(socket),
        prefix_(std::move(prefix)),
        remaining_(total),
        timeout_(timeout) {}

  bool next(std::string& out) override {
    if (remaining_ == 0) {
      return false;
    }
    if (!prefix_.empty()) {
      out = std::move(prefix_);
      prefix_.clear();
      remaining_ -= out.size();
      return true;
    }
    char buffer[16384];
    const std::size_t n =
        socket_.readSome(buffer, std::min(sizeof buffer, remaining_), timeout_);
    if (n == 0) {
      throw IoFailure(
          "client closed the connection before the request body was complete",
          0);
    }
    out.assign(buffer, n);
    remaining_ -= n;
    return true;
  }

 private:
  Socket& socket_;
  std::string prefix_;
  std::size_t remaining_;
  std::chrono::milliseconds timeout_;
};

class DirectSink final : public ResponseSink {
 public:
  DirectSink(Socket& socket, bool& headSent, std::chrono::milliseconds timeout)
      : socket_(socket), headSent_(headSent), timeout_(timeout) {}

  void onHead(const ResponseHead& head) override {
    const std::string text = head.serializeForClient();
    headSent_ = true;
    socket_.writeAll(text, timeout_);
  }

  void onBody(const char* data, std::size_t size) override {
    socket_.writeAll(std::string_view(data, size), timeout_);
  }

  void onComplete() override {}

 private:
  Socket& socket_;
  bool& headSent_;
  std::chrono::milliseconds timeout_;
};

}  // namespace

ClientSession::ClientSession(Socket socket,
                             Cache& cache,
                             Fetcher& fetcher,
                             ThreadPool& fetchPool,
                             const StopSignal& stop,
                             const Config& config)
    : socket_(std::move(socket)),
      cache_(cache),
      fetcher_(fetcher),
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
    log.write(LogLevel::Warn, std::string("client I/O failure: ") + e.what(),
              id, url_);
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

void ClientSession::serveCached(const HttpRequest& request) {
  auto acquired = cache_.acquire(request.url);
  if (acquired.created) {
    Logger::instance().print(LogLevel::Debug, "cache miss: ", request.url);
    auto entry = acquired.entry;
    auto copy = std::make_shared<const HttpRequest>(request);
    Fetcher* fetcher = &fetcher_;
    const bool scheduled = fetchPool_.trySubmit(
        [fetcher, entry, copy] { fetcher->run(*copy, entry); });
    if (!scheduled) {
      Logger::instance().write(LogLevel::Error,
                               "failed to schedule upstream fetch",
                               socket_.id(), request.url);
      entry->fail(500, "failed to schedule upstream fetch");
      cache_.remove(entry);
    }
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
  ClientBodySource body(socket_, request.bodyPrefix,
                        request.contentLength.value_or(0),
                        config_.clientTimeout);
  DirectSink sink(socket_, headSent_, config_.clientTimeout);
  client.exchange(request, &body, sink);
}
