#include "Logger.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <functional>
#include <string>
#include <thread>

namespace {
constexpr const char* kNames[] = {"ERROR", "WARN", "NOTICE", "LOG", "DEBUG"};
}

Logger& Logger::instance() {
  static Logger logger;
  return logger;
}

void Logger::setLevel(LogLevel level) noexcept {
  level_.store(level);
}

bool Logger::enabled(LogLevel level) const noexcept {
  return static_cast<int>(level) <= static_cast<int>(level_.load());
}

void Logger::write(LogLevel level, std::string_view message) {
  if (!enabled(level)) {
    return;
  }
  using namespace std::chrono;
  const auto now = system_clock::now();
  const std::time_t seconds = system_clock::to_time_t(now);
  const long millis = static_cast<long>(
      duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000);
  std::tm tm{};
  char stamp[32] = "?";
  if (localtime_r(&seconds, &tm) != nullptr) {
    std::strftime(stamp, sizeof stamp, "%Y-%m-%d %H:%M:%S", &tm);
  }
  const std::size_t tid =
      std::hash<std::thread::id>{}(std::this_thread::get_id()) % 100000;

  std::lock_guard lock(mutex_);
  std::fprintf(stdout, "%s.%03ld [%-6s] [t%05zu] %.*s\n", stamp, millis,
               kNames[static_cast<int>(level)], tid,
               static_cast<int>(message.size()), message.data());
  std::fflush(stdout);
}

void Logger::write(LogLevel level,
                   std::string_view message,
                   int socketId,
                   std::string_view url) {
  if (!enabled(level)) {
    return;
  }
  try {
    std::string full(message);
    full += " [socket=" + std::to_string(socketId) + " url=";
    full += url.empty() ? std::string_view("-") : url;
    full += "]";
    write(level, full);
  } catch (...) {
  }
}
