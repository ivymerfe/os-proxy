#include "Fetcher.hpp"

#include <new>
#include <string>

#include "Errors.hpp"
#include "Logger.hpp"
#include "ResponseSink.hpp"
#include "UpstreamClient.hpp"

namespace {

class EntrySink final : public ResponseSink {
 public:
  EntrySink(Cache& cache, std::shared_ptr<CacheEntry> entry)
      : cache_(cache), entry_(std::move(entry)) {}

  void onHead(const ResponseHead& head) override {
    const bool tooLarge =
        head.contentLength && *head.contentLength > cache_.maxEntrySize();
    if (head.status != 200 || tooLarge) {
      detach();
    }
    entry_->setHead(head.serializeForClient());
  }

  void onBody(const char* data, std::size_t size) override {
    if (entry_->cacheable() && !entry_->tryReserve(size)) {
      detach();
    }
    if (!entry_->append(data, size)) {
      throw FetchCancelled{};
    }
  }

  void onComplete() override { entry_->complete(); }

 private:
  void detach() {
    entry_->markUncacheable();
    cache_.remove(entry_);
  }

  Cache& cache_;
  std::shared_ptr<CacheEntry> entry_;
};

}  // namespace

void Fetcher::run(const HttpRequest& request,
                  const std::shared_ptr<CacheEntry>& entry) {
  int status = 500;
  int socketId = -1;
  LogLevel level = LogLevel::Error;
  std::string message;
  try {
    if (stop_.requested()) {
      throw IoStopped("proxy is shutting down");
    }
    UpstreamClient client(stop_, timeout_);
    EntrySink sink(cache_, entry);
    client.exchange(request, nullptr, sink);
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
    status = 500;
    message = "transfer cancelled: no clients left";
    level = LogLevel::Debug;
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
  cache_.remove(entry);
}
