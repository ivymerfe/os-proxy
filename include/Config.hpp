#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "Logger.hpp"

struct Config {
  std::uint16_t port{8080};
  std::size_t clientThreads{4};
  std::size_t cacheInitialSize{1024 * 1024};
  std::size_t cacheMaxSize{10 * 1024 * 1024};
  std::chrono::seconds cacheTtl{5};
  std::size_t queueSize{128};
  LogLevel logLevel{LogLevel::Log};
  std::chrono::milliseconds upstreamTimeout{10000};
  std::chrono::milliseconds clientTimeout{30000};
};

class ConfigParser {
 public:
  static std::optional<Config> parse(int argc, char** argv);
  static std::string usage(std::string_view program);

 private:
  static unsigned long long parseNumber(std::string_view text,
                                        std::string_view name,
                                        unsigned long long min,
                                        unsigned long long max);
  static std::size_t parseSize(std::string_view text, std::string_view name);
  static LogLevel parseLevel(std::string_view text);
};
