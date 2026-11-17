#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "Net.hpp"

constexpr std::size_t kMaxRequestBody = 16 * 1024 * 1024;

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

class RequestParser {
public:
    static constexpr std::size_t kMaxHeaderBytes = 16 * 1024;

    explicit RequestParser(int socketId) : socketId_(socketId) {}

    bool feed(const char* data, std::size_t size);
    HttpRequest take();

private:
    int socketId_;
    std::string buffer_;
    std::size_t previous_{0};
    std::optional<HttpRequest> request_;
};

using BodySource = std::function<bool(std::string&)>;
using HeadHandler = std::function<void(const ResponseHead&)>;
using DataHandler = std::function<void(const char*, std::size_t)>;

class UpstreamClient {
 public:
  UpstreamClient(const StopSignal& stop, std::chrono::milliseconds timeout)
      : stop_(stop), timeout_(timeout) {}

  void exchange(const HttpRequest& request,
                const BodySource& body,
                const HeadHandler& onHead,
                const DataHandler& onData);

 private:
  Socket openConnection(const HttpRequest& request);
  static std::string buildRequestHead(const HttpRequest& request);
  ResponseHead readHead(Socket& socket, std::string& buffer);
  void readBody(Socket& socket,
                const ResponseHead& head,
                std::string& buffer,
                const DataHandler& onData);
  std::size_t receive(Socket& socket, char* buffer, std::size_t size);
  void transmit(Socket& socket, std::string_view data);

  const StopSignal& stop_;
  std::chrono::milliseconds timeout_;
};
