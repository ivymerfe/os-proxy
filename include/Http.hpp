#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "Socket.hpp"

struct Header {
  std::string name;
  std::string value;
};

bool iequals(std::string_view a, std::string_view b) noexcept;
bool isHopByHop(std::string_view name) noexcept;
std::optional<std::size_t> parseDecimal(std::string_view text) noexcept;

struct HttpRequest {
  std::string method;
  std::string host;
  std::string port{"80"};
  std::string path;
  std::string url;
  int minorVersion{1};
  std::vector<Header> headers;
  std::optional<std::size_t> contentLength;
  std::string bodyPrefix;

  std::string authority() const;
};

struct ResponseHead {
  int status{0};
  std::string reason;
  std::vector<Header> headers;
  std::optional<std::size_t> contentLength;
  bool chunked{false};

  std::string serializeForClient() const;
};

class HttpResponses {
 public:
  static std::string_view reason(int status) noexcept;
  static std::string error(int status, std::string_view message);
};

class RequestReader {
 public:
  static constexpr std::size_t kMaxHeaderBytes = 16 * 1024;

  RequestReader(Socket& socket, std::chrono::milliseconds timeout)
      : socket_(socket), timeout_(timeout) {}

  std::optional<HttpRequest> read();

 private:
  Socket& socket_;
  std::chrono::milliseconds timeout_;
};
