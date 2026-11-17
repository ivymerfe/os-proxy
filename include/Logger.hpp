#pragma once

#include <atomic>
#include <mutex>
#include <sstream>
#include <string_view>
#include <utility>

enum class LogLevel { Error = 0, Warn, Notice, Log, Debug };

class Logger {
 public:
  static Logger& instance();

  void setLevel(LogLevel level) noexcept;
  bool enabled(LogLevel level) const noexcept;

  void write(LogLevel level, std::string_view message);
  void write(LogLevel level,
             std::string_view message,
             int socketId,
             std::string_view url);

  template <typename... Args>
  void print(LogLevel level, Args&&... args) {
    if (!enabled(level)) {
      return;
    }
    try {
      std::ostringstream stream;
      (stream << ... << std::forward<Args>(args));
      write(level, stream.str());
    } catch (...) {
    }
  }

 private:
  Logger() = default;

  std::atomic<LogLevel> level_{LogLevel::Log};
  std::mutex mutex_;
};
