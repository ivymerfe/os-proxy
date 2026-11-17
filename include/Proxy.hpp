#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Cache.hpp"
#include "Config.hpp"
#include "Http.hpp"
#include "Net.hpp"
#include "Runtime.hpp"

class Session {
 public:
  using Clock = std::chrono::steady_clock;

  Session(Socket socket,
          Cache& cache,
          ThreadPool& fetchPool,
          const StopSignal& stop,
          const Config& config,
          std::function<void()> notify);

  int fd() const noexcept { return socket_.id(); }
  bool done() const noexcept { return state_ == State::Done; }
  short events() const noexcept;
  Clock::time_point deadline() const noexcept { return deadline_; }

  void advance();
  void onTimeout();

 private:
  enum class State {
    ReadingRequest,
    ReadingBody,
    WaitingHead,
    Streaming,
    Flushing,
    Done
  };

  bool step();
  bool readRequest();
  bool readBody();
  bool waitHead();
  bool stream();
  bool flush();
  void startGet();
  void startDirect();
  void startFetch(BodySource body, const std::shared_ptr<CacheEntry>& entry);
  void checkPeer();
  void report();
  void fail(int status, std::string_view message);
  void send(std::string data);
  void touch();
  void finish() noexcept;

  Socket socket_;
  RequestParser parser_;
  Cache& cache_;
  ThreadPool& fetchPool_;
  const StopSignal& stop_;
  const Config& config_;
  std::function<void()> notify_;
  State state_{State::ReadingRequest};
  std::optional<HttpRequest> request_;
  std::unique_ptr<EntryReader> reader_;
  std::string body_;
  std::string out_;
  std::size_t outPos_{0};
  bool received_{false};
  bool headSent_{false};
  std::string url_;
  Clock::time_point deadline_;
};

class Reactor {
 public:
  Reactor(Cache& cache,
          ThreadPool& fetchPool,
          const StopSignal& stop,
          const Config& config);
  ~Reactor();
  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;

  void start();
  void stop();
  bool adopt(UniqueFd& fd);
  void wake(std::uint64_t id) noexcept;
  std::size_t load() const noexcept { return load_.load(); }

 private:
  void loop();
  void adoptNow(UniqueFd&& fd);
  void drainWakeups();
  void signal() noexcept;

  Cache& cache_;
  ThreadPool& fetchPool_;
  const StopSignal& stop_;
  const Config& config_;
  UniqueFd wakeRead_;
  UniqueFd wakeWrite_;
  std::mutex mutex_;
  std::deque<UniqueFd> incoming_;
  std::unordered_set<std::uint64_t> ready_;
  std::atomic<std::size_t> load_{0};
  std::atomic<bool> alive_{true};
  std::atomic<bool> quit_{false};
  std::uint64_t nextId_{1};
  std::unordered_map<std::uint64_t, std::unique_ptr<Session>> sessions_;
  std::thread thread_;
};

class Server {
 public:
  Server(const Config& config, StopSignal& stop)
      : config_(config), stop_(stop) {}

  int run();

 private:
  UniqueFd createListener();
  void acceptLoop();
  void acceptOne();
  void handleDescriptorExhaustion();
  void reject(UniqueFd fd, int status, std::string_view message);
  Reactor& pickReactor();
  std::size_t totalLoad() const;

  Config config_;
  StopSignal& stop_;
  UniqueFd listener_;
  UniqueFd spare_;
  std::unique_ptr<Cache> cache_;
  std::unique_ptr<ThreadPool> fetchPool_;
  std::vector<std::unique_ptr<Reactor>> reactors_;
};
