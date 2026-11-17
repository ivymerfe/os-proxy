#include "UpstreamClient.hpp"

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <memory>

#include "ChunkedDecoder.hpp"
#include "Errors.hpp"
#include "Platform.hpp"

#include "parser/picohttpparser.h"

namespace {

constexpr std::size_t kMaxHeaders = 100;

bool isConditionalOrFraming(std::string_view name) {
  static constexpr std::string_view kNames[] = {
      "range",           "if-range",      "if-modified-since",
      "if-none-match",   "if-match",      "if-unmodified-since",
      "accept-encoding", "content-length"};
  return std::any_of(std::begin(kNames), std::end(kNames),
                     [&](std::string_view n) { return iequals(name, n); });
}

}  // namespace

std::size_t UpstreamClient::receive(Socket& socket,
                                    char* buffer,
                                    std::size_t size) {
  try {
    return socket.readSome(buffer, size, timeout_);
  } catch (const IoTimeout&) {
    throw HttpError(504, "timeout while reading from upstream server",
                    socket.id());
  } catch (const IoFailure& e) {
    throw HttpError(502, std::string("upstream read failed: ") + e.what(),
                    socket.id());
  }
}

void UpstreamClient::transmit(Socket& socket, std::string_view data) {
  try {
    socket.writeAll(data, timeout_);
  } catch (const IoTimeout&) {
    throw HttpError(504, "timeout while writing to upstream server",
                    socket.id());
  } catch (const IoFailure& e) {
    throw HttpError(502, std::string("upstream write failed: ") + e.what(),
                    socket.id());
  }
}

Socket UpstreamClient::openConnection(const HttpRequest& request) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* raw = nullptr;
  const int rc =
      ::getaddrinfo(request.host.c_str(), request.port.c_str(), &hints, &raw);
  if (rc != 0) {
    throw HttpError(502, "DNS resolution failed for " + request.host + ": " +
                             ::gai_strerror(rc));
  }
  std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> list(raw,
                                                            &::freeaddrinfo);

  int status = 502;
  int lastSocket = -1;
  std::string message = "no usable address for " + request.host;
  for (const addrinfo* ai = list.get(); ai != nullptr; ai = ai->ai_next) {
    const int fd = platform::makeSocket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      const int e = errno;
      status = (e == EMFILE || e == ENFILE || e == ENOBUFS || e == ENOMEM)
                   ? 500
                   : 502;
      message = describeErrno("socket", e);
      continue;
    }
    Socket socket(UniqueFd(fd), stop_);
    lastSocket = fd;
    int err = 0;
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) < 0) {
      err = errno;
      if (err == EINPROGRESS || err == EINTR) {
        switch (socket.wait(POLLOUT, timeout_)) {
          case WaitResult::Stopped:
            throw IoStopped("proxy is shutting down");
          case WaitResult::Timeout:
            err = ETIMEDOUT;
            break;
          case WaitResult::Ready: {
            socklen_t length = sizeof err;
            err = 0;
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &length) < 0) {
              err = errno;
            }
            break;
          }
        }
      }
    }
    if (err == 0) {
      return socket;
    }
    status = err == ETIMEDOUT ? 504 : 502;
    message =
        describeErrno("connect to " + request.host + ":" + request.port, err);
  }
  throw HttpError(status, message, lastSocket);
}

std::string UpstreamClient::buildRequestHead(const HttpRequest& request) {
  const bool isGet = request.method == "GET";
  std::string out;
  out.reserve(512);
  out += request.method + " " + request.path + " HTTP/1.1\r\n";
  out += "Host: " + request.authority() + "\r\n";
  for (const auto& header : request.headers) {
    if (isHopByHop(header.name) || iequals(header.name, "Host") ||
        iequals(header.name, "Expect")) {
      continue;
    }
    if (isGet && isConditionalOrFraming(header.name)) {
      continue;
    }
    out += header.name + ": " + header.value + "\r\n";
  }
  if (isGet) {
    out += "Accept-Encoding: identity\r\n";
  } else if (!request.contentLength &&
             (request.method == "POST" || request.method == "PUT")) {
    out += "Content-Length: 0\r\n";
  }
  out += "Connection: close\r\n\r\n";
  return out;
}

ResponseHead UpstreamClient::readHead(Socket& socket, std::string& buffer) {
  const int id = socket.id();
  char chunk[4096];
  std::size_t previous = 0;
  for (;;) {
    if (buffer.size() >= kMaxHeadBytes) {
      throw HttpError(502, "upstream response headers are too large", id);
    }
    const std::size_t got = receive(
        socket, chunk, std::min(sizeof chunk, kMaxHeadBytes - buffer.size()));
    if (got == 0) {
      throw HttpError(
          502, "upstream closed the connection before sending response headers",
          id);
    }
    buffer.append(chunk, got);

    for (;;) {
      phr_header raw[kMaxHeaders];
      std::size_t count = kMaxHeaders;
      int minor = 0;
      int status = 0;
      const char* message = nullptr;
      std::size_t messageLen = 0;
      const int parsed =
          phr_parse_response(buffer.data(), buffer.size(), &minor, &status,
                             &message, &messageLen, raw, &count, previous);
      if (parsed == -2) {
        previous = buffer.size();
        break;
      }
      if (parsed == -1) {
        throw HttpError(502, "upstream returned an invalid HTTP response", id);
      }
      if (status >= 100 && status < 200) {
        if (status == 101) {
          throw HttpError(502, "upstream attempted a protocol switch", id);
        }
        buffer.erase(0, static_cast<std::size_t>(parsed));
        previous = 0;
        continue;
      }

      ResponseHead head;
      head.status = status;
      head.reason.assign(message, messageLen);
      for (std::size_t i = 0; i < count; ++i) {
        if (raw[i].name == nullptr) {
          throw HttpError(
              502,
              "upstream returned an invalid HTTP response (header folding)",
              id);
        }
        const std::string_view value(raw[i].value, raw[i].value_len);
        Header header{std::string(raw[i].name, raw[i].name_len),
                      std::string(value)};
        if (iequals(header.name, "Content-Length")) {
          const auto length = parseDecimal(value);
          if (!length ||
              (head.contentLength && *head.contentLength != *length)) {
            throw HttpError(502, "upstream returned an invalid Content-Length",
                            id);
          }
          head.contentLength = length;
        } else if (iequals(header.name, "Transfer-Encoding")) {
          std::string_view token = value;
          while (!token.empty() && token.front() == ' ') {
            token.remove_prefix(1);
          }
          while (!token.empty() && token.back() == ' ') {
            token.remove_suffix(1);
          }
          if (!iequals(token, "chunked")) {
            throw HttpError(
                502, "upstream used an unsupported Transfer-Encoding", id);
          }
          head.chunked = true;
        }
        head.headers.push_back(std::move(header));
      }
      buffer.erase(0, static_cast<std::size_t>(parsed));
      return head;
    }
  }
}

void UpstreamClient::readBody(Socket& socket,
                              const ResponseHead& head,
                              std::string& buffer,
                              ResponseSink& sink) {
  if (head.status == 204 || head.status == 304) {
    return;
  }
  const int id = socket.id();
  const bool fixed = !head.chunked && head.contentLength.has_value();
  std::size_t remaining = head.contentLength.value_or(0);
  ChunkedDecoder decoder;

  const auto feed = [&](std::string_view data) -> bool {
    if (head.chunked) {
      try {
        decoder.feed(data, [&](const char* bytes, std::size_t size) {
          if (size != 0) {
            sink.onBody(bytes, size);
          }
        });
      } catch (const std::invalid_argument& e) {
        throw HttpError(
            502, std::string("upstream sent invalid chunked body: ") + e.what(),
            id);
      }
      return decoder.done();
    }
    if (fixed) {
      const std::size_t n = std::min(remaining, data.size());
      if (n != 0) {
        sink.onBody(data.data(), n);
      }
      remaining -= n;
      return remaining == 0;
    }
    if (!data.empty()) {
      sink.onBody(data.data(), data.size());
    }
    return false;
  };

  if (fixed && remaining == 0) {
    return;
  }
  if (!buffer.empty() && feed(buffer)) {
    return;
  }
  buffer.clear();
  char chunk[16384];
  for (;;) {
    const std::size_t got = receive(socket, chunk, sizeof chunk);
    if (got == 0) {
      if (head.chunked || fixed) {
        throw HttpError(502,
                        "upstream closed the connection before the full "
                        "response was received",
                        id);
      }
      return;
    }
    if (feed(std::string_view(chunk, got))) {
      return;
    }
  }
}

void UpstreamClient::exchange(const HttpRequest& request,
                              RequestBodySource* body,
                              ResponseSink& sink) {
  Socket socket = openConnection(request);
  transmit(socket, buildRequestHead(request));
  if (body != nullptr) {
    std::string chunk;
    while (body->next(chunk)) {
      transmit(socket, chunk);
    }
  }
  std::string buffer;
  const ResponseHead head = readHead(socket, buffer);
  sink.onHead(head);
  readBody(socket, head, buffer, sink);
  sink.onComplete();
}
