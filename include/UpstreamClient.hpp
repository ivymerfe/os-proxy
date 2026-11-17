#pragma once

#include <chrono>
#include <string>

#include "Http.hpp"
#include "ResponseSink.hpp"
#include "Socket.hpp"

class UpstreamClient {
 public:
  UpstreamClient(const StopSignal& stop, std::chrono::milliseconds timeout)
      : stop_(stop), timeout_(timeout) {}

  void exchange(const HttpRequest& request,
                RequestBodySource* body,
                ResponseSink& sink);

 private:
  static constexpr std::size_t kMaxHeadBytes = 16 * 1024;

  Socket openConnection(const HttpRequest& request);
  static std::string buildRequestHead(const HttpRequest& request);
  ResponseHead readHead(Socket& socket, std::string& buffer);
  void readBody(Socket& socket,
                const ResponseHead& head,
                std::string& buffer,
                ResponseSink& sink);
  std::size_t receive(Socket& socket, char* buffer, std::size_t size);
  void transmit(Socket& socket, std::string_view data);

  const StopSignal& stop_;
  std::chrono::milliseconds timeout_;
};
