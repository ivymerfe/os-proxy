#include "Config.hpp"

#include <getopt.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>

unsigned long long ConfigParser::parseNumber(std::string_view text,
                                             std::string_view name,
                                             unsigned long long min,
                                             unsigned long long max) {
  unsigned long long value = 0;
  const auto [ptr, ec] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || ec != std::errc{} || ptr != text.data() + text.size() ||
      value < min || value > max) {
    throw std::invalid_argument("invalid value for " + std::string(name) +
                                ": '" + std::string(text) + "'");
  }
  return value;
}

std::size_t ConfigParser::parseSize(std::string_view text,
                                    std::string_view name) {
  unsigned long long multiplier = 1;
  std::string_view digits = text;
  if (!digits.empty()) {
    switch (std::toupper(static_cast<unsigned char>(digits.back()))) {
      case 'K':
        multiplier = 1ULL << 10;
        digits.remove_suffix(1);
        break;
      case 'M':
        multiplier = 1ULL << 20;
        digits.remove_suffix(1);
        break;
      case 'G':
        multiplier = 1ULL << 30;
        digits.remove_suffix(1);
        break;
      default:
        break;
    }
  }
  const unsigned long long limit =
      std::numeric_limits<std::size_t>::max() / multiplier;
  return static_cast<std::size_t>(parseNumber(digits, name, 1, limit) *
                                  multiplier);
}

LogLevel ConfigParser::parseLevel(std::string_view text) {
  std::string upper(text);
  std::transform(upper.begin(), upper.end(), upper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  if (upper == "ERROR") {
    return LogLevel::Error;
  }
  if (upper == "WARN") {
    return LogLevel::Warn;
  }
  if (upper == "NOTICE") {
    return LogLevel::Notice;
  }
  if (upper == "LOG") {
    return LogLevel::Log;
  }
  if (upper == "DEBUG") {
    return LogLevel::Debug;
  }
  throw std::invalid_argument("invalid log level: '" + std::string(text) + "'");
}

std::optional<Config> ConfigParser::parse(int argc, char** argv) {
  enum { OptInitial = 1000, OptMax, OptTtl, OptQueue, OptLevel };
  static const option options[] = {
      {"port", required_argument, nullptr, 'p'},
      {"max-client-threads", required_argument, nullptr, 't'},
      {"help", no_argument, nullptr, 'h'},
      {"cache-initial-size", required_argument, nullptr, OptInitial},
      {"cache-max-size", required_argument, nullptr, OptMax},
      {"cache-ttl", required_argument, nullptr, OptTtl},
      {"queue-size", required_argument, nullptr, OptQueue},
      {"log-level", required_argument, nullptr, OptLevel},
      {nullptr, 0, nullptr, 0}};

  Config config;
  optind = 1;
#ifdef __APPLE__
  optreset = 1;
#endif
  opterr = 0;
  int c = 0;
  while ((c = getopt_long(argc, argv, ":p:t:h", options, nullptr)) != -1) {
    switch (c) {
      case 'p':
        config.port =
            static_cast<std::uint16_t>(parseNumber(optarg, "--port", 1, 65535));
        break;
      case 't':
        config.clientThreads = static_cast<std::size_t>(
            parseNumber(optarg, "--max-client-threads", 1, 4096));
        break;
      case 'h':
        return std::nullopt;
      case OptInitial:
        config.cacheInitialSize = parseSize(optarg, "--cache-initial-size");
        break;
      case OptMax:
        config.cacheMaxSize = parseSize(optarg, "--cache-max-size");
        break;
      case OptTtl:
        config.cacheTtl = std::chrono::seconds(
            parseNumber(optarg, "--cache-ttl", 0, 86400ULL * 365));
        break;
      case OptQueue:
        config.queueSize = static_cast<std::size_t>(
            parseNumber(optarg, "--queue-size", 1, 1000000));
        break;
      case OptLevel:
        config.logLevel = parseLevel(optarg);
        break;
      case ':':
        throw std::invalid_argument(std::string("missing value for option ") +
                                    argv[optind - 1]);
      default:
        throw std::invalid_argument(std::string("unknown option ") +
                                    argv[optind - 1]);
    }
  }
  if (optind != argc) {
    throw std::invalid_argument(std::string("unexpected argument: ") +
                                argv[optind]);
  }
  if (config.cacheInitialSize > config.cacheMaxSize) {
    throw std::invalid_argument(
        "--cache-initial-size must not exceed --cache-max-size");
  }
  return config;
}

std::string ConfigParser::usage(std::string_view program) {
  std::string text = "Usage: " + std::string(program) + " [options]\n\n";
  text +=
      "  -p, --port N                порт прокси (по умолчанию 8080)\n"
      "  -t, --max-client-threads N  размер пула клиентских потоков (по "
      "умолчанию 4)\n"
      "  -h, --help                  показать эту справку\n"
      "      --cache-initial-size S  начальный размер кэша, байты или K/M/G "
      "(по умолчанию 1M)\n"
      "      --cache-max-size S      максимальный размер кэша, байты или K/M/G "
      "(по умолчанию 10M)\n"
      "      --cache-ttl SEC         время жизни записи кэша в секундах (по "
      "умолчанию 5)\n"
      "      --queue-size N          размер очереди ожидающих сессий (по "
      "умолчанию 128)\n"
      "      --log-level LEVEL       ERROR|WARN|NOTICE|LOG|DEBUG (по умолчанию "
      "LOG)\n";
  return text;
}
