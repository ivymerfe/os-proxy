#include "Http.hpp"

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <memory>

#include "parser/picohttpparser.h"

namespace {

constexpr std::size_t kMaxHeaders = 100;
constexpr std::size_t kMaxHeadBytes = 16 * 1024;

std::string_view trim(std::string_view text) {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
    text.remove_suffix(1);
  }
  return text;
}

std::string lower(std::string text) {
  for (char& c : text) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return text;
}

bool isConditionalOrFraming(std::string_view name) {
  static constexpr std::string_view kNames[] = {
      "range",           "if-range",      "if-modified-since",
      "if-none-match",   "if-match",      "if-unmodified-since",
      "accept-encoding", "content-length"};
  return std::any_of(std::begin(kNames), std::end(kNames),
                     [&](std::string_view n) { return iequals(name, n); });
}

void splitAuthority(std::string_view authority,
                    std::string& host,
                    std::string& port,
                    int socketId) {
  port = "80";
  if (const auto at = authority.rfind('@'); at != std::string_view::npos) {
    authority.remove_prefix(at + 1);
  }
  std::string_view hostPart = authority;
  std::string_view portPart;
  if (!authority.empty() && authority.front() == '[') {
    const auto end = authority.find(']');
    if (end == std::string_view::npos) {
      throw HttpError(400, "invalid host in request", socketId);
    }
    hostPart = authority.substr(1, end - 1);
    const auto rest = authority.substr(end + 1);
    if (!rest.empty()) {
      if (rest.front() != ':') {
        throw HttpError(400, "invalid host in request", socketId);
      }
      portPart = rest.substr(1);
    }
  } else if (const auto colon = authority.rfind(':');
             colon != std::string_view::npos) {
    hostPart = authority.substr(0, colon);
    portPart = authority.substr(colon + 1);
  }
  if (hostPart.empty()) {
    throw HttpError(400, "missing host in request", socketId);
  }
  if (!portPart.empty()) {
    unsigned value = 0;
    const auto [ptr, ec] = std::from_chars(
        portPart.data(), portPart.data() + portPart.size(), value);
    if (ec != std::errc{} || ptr != portPart.data() + portPart.size() ||
        value == 0 || value > 65535) {
      throw HttpError(400, "invalid port in request", socketId);
    }
    port = std::to_string(value);
  }
  host = lower(std::string(hostPart));
}

HttpRequest buildRequest(std::string_view method,
                         std::string_view target,
                         int minor,
                         const phr_header* raw,
                         std::size_t count,
                         std::string_view rest,
                         int socketId) {
  HttpRequest request;
  request.method.assign(method);
  if (request.method != "GET" && request.method != "PUT" &&
      request.method != "POST" && request.method != "DELETE") {
    throw HttpError(501, "method not supported: " + request.method, socketId);
  }
  request.minorVersion = minor;

  std::string hostHeader;
  for (std::size_t i = 0; i < count; ++i) {
    if (raw[i].name == nullptr) {
      throw HttpError(400, "obsolete header line folding is not supported",
                      socketId);
    }
    Header header{
        std::string(raw[i].name, raw[i].name_len),
        std::string(trim(std::string_view(raw[i].value, raw[i].value_len)))};
    if (iequals(header.name, "Host")) {
      hostHeader = header.value;
    } else if (iequals(header.name, "Content-Length")) {
      const auto length = parseDecimal(header.value);
      if (!length ||
          (request.contentLength && *request.contentLength != *length)) {
        throw HttpError(400, "invalid Content-Length header", socketId);
      }
      request.contentLength = length;
    } else if (iequals(header.name, "Transfer-Encoding")) {
      throw HttpError(501, "Transfer-Encoding in requests is not supported",
                      socketId);
    }
    request.headers.push_back(std::move(header));
  }

  std::string authority;
  if (iequals(target.substr(0, 7), "http://")) {
    const auto afterScheme = target.substr(7);
    const auto split = afterScheme.find_first_of("/?");
    authority.assign(afterScheme.substr(0, split));
    if (split == std::string_view::npos) {
      request.path = "/";
    } else {
      request.path.assign(afterScheme.substr(split));
      if (request.path.front() == '?') {
        request.path.insert(request.path.begin(), '/');
      }
    }
  } else if (iequals(target.substr(0, 8), "https://")) {
    throw HttpError(501, "https:// targets are not supported", socketId);
  } else if (!target.empty() && target.front() == '/') {
    if (hostHeader.empty()) {
      throw HttpError(400, "missing Host header", socketId);
    }
    authority = hostHeader;
    request.path.assign(target);
  } else {
    throw HttpError(400, "unsupported request target", socketId);
  }

  splitAuthority(authority, request.host, request.port, socketId);
  request.url = "http://" + request.authority() + request.path;
  const std::size_t prefix =
      std::min(rest.size(), request.contentLength.value_or(0));
  request.bodyPrefix.assign(rest.substr(0, prefix));
  return request;
}

class ChunkedDecoder {
 public:
  bool done() const noexcept { return state_ == State::Done; }

  void feed(std::string_view input, const DataHandler& emit) {
    while (!input.empty() && state_ != State::Done) {
      if (state_ == State::Data) {
        const std::size_t n = std::min(input.size(), remaining_);
        emit(input.data(), n);
        input.remove_prefix(n);
        remaining_ -= n;
        if (remaining_ == 0) {
          state_ = State::DataEnd;
        }
        continue;
      }
      const char c = input.front();
      input.remove_prefix(1);
      if (c != '\n') {
        line_.push_back(c);
        if (line_.size() > 4096) {
          throw std::invalid_argument("chunk line too long");
        }
        continue;
      }
      if (!line_.empty() && line_.back() == '\r') {
        line_.pop_back();
      }
      handleLine();
      line_.clear();
    }
  }

 private:
  enum class State { SizeLine, Data, DataEnd, Trailers, Done };

  void handleLine() {
    switch (state_) {
      case State::SizeLine: {
        const std::string_view text =
            std::string_view(line_).substr(0, line_.find(';'));
        std::size_t size = 0;
        const auto [ptr, ec] =
            std::from_chars(text.data(), text.data() + text.size(), size, 16);
        if (text.empty() || ec != std::errc{} ||
            ptr != text.data() + text.size()) {
          throw std::invalid_argument("invalid chunk size");
        }
        remaining_ = size;
        state_ = size == 0 ? State::Trailers : State::Data;
        break;
      }
      case State::DataEnd:
        if (!line_.empty()) {
          throw std::invalid_argument("missing chunk terminator");
        }
        state_ = State::SizeLine;
        break;
      case State::Trailers:
        if (line_.empty()) {
          state_ = State::Done;
        }
        break;
      default:
        break;
    }
  }

  State state_{State::SizeLine};
  std::size_t remaining_{0};
  std::string line_;
};

}  // namespace

bool iequals(std::string_view a, std::string_view b) noexcept {
  return a.size() == b.size() &&
         std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
           return std::tolower(static_cast<unsigned char>(x)) ==
                  std::tolower(static_cast<unsigned char>(y));
         });
}

bool isHopByHop(std::string_view name) noexcept {
  static constexpr std::string_view kNames[] = {"connection",
                                                "keep-alive",
                                                "proxy-authenticate",
                                                "proxy-authorization",
                                                "proxy-connection",
                                                "te",
                                                "trailer",
                                                "transfer-encoding",
                                                "upgrade"};
  return std::any_of(std::begin(kNames), std::end(kNames),
                     [&](std::string_view n) { return iequals(name, n); });
}

std::optional<std::size_t> parseDecimal(std::string_view text) noexcept {
  text = trim(text);
  std::size_t value = 0;
  const auto [ptr, ec] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || ec != std::errc{} || ptr != text.data() + text.size()) {
    return std::nullopt;
  }
  return value;
}

std::string HttpRequest::authority() const {
  std::string out =
      host.find(':') != std::string::npos ? "[" + host + "]" : host;
  if (port != "80") {
    out += ":" + port;
  }
  return out;
}

std::string ResponseHead::serializeForClient() const {
  std::string out = "HTTP/1.1 " + std::to_string(status) + " ";
  out += reason.empty() ? std::string(HttpResponses::reason(status)) : reason;
  out += "\r\n";
  for (const auto& header : headers) {
    if (isHopByHop(header.name) ||
        (chunked && iequals(header.name, "Content-Length"))) {
      continue;
    }
    out += header.name + ": " + header.value + "\r\n";
  }
  out += "Connection: close\r\n\r\n";
  return out;
}

std::string_view HttpResponses::reason(int status) noexcept {
  switch (status) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 408:
      return "Request Timeout";
    case 500:
      return "Internal Server Error";
    case 501:
      return "Not Implemented";
    case 502:
      return "Bad Gateway";
    case 503:
      return "Service Unavailable";
    case 504:
      return "Gateway Timeout";
    default:
      return "Unknown";
  }
}

std::string HttpResponses::error(int status, std::string_view message) {
  std::string body = std::to_string(status) + " " +
                     std::string(reason(status)) + ": " + std::string(message) +
                     "\n";
  std::string out = "HTTP/1.1 " + std::to_string(status) + " " +
                    std::string(reason(status)) + "\r\n";
  out += "Content-Type: text/plain; charset=utf-8\r\n";
  out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
  out += "Connection: close\r\n\r\n";
  out += body;
  return out;
}

std::optional<HttpRequest> RequestReader::read() {
  std::string buffer;
  std::size_t previous = 0;
  char chunk[4096];
  for (;;) {
    if (buffer.size() >= kMaxHeaderBytes) {
      throw HttpError(400, "request headers exceed 16 KB", socket_.id());
    }
    const std::size_t want =
        std::min(sizeof chunk, kMaxHeaderBytes - buffer.size());
    const std::size_t got = socket_.readSome(chunk, want, timeout_);
    if (got == 0) {
      if (buffer.empty()) {
        return std::nullopt;
      }
      throw HttpError(400, "connection closed before the request was complete",
                      socket_.id());
    }
    buffer.append(chunk, got);

    const char* method = nullptr;
    const char* path = nullptr;
    std::size_t methodLen = 0;
    std::size_t pathLen = 0;
    std::size_t headerCount = kMaxHeaders;
    int minor = 0;
    phr_header raw[kMaxHeaders];
    const int parsed =
        phr_parse_request(buffer.data(), buffer.size(), &method, &methodLen,
                          &path, &pathLen, &minor, raw, &headerCount, previous);
    if (parsed > 0) {
      return buildRequest(
          std::string_view(method, methodLen), std::string_view(path, pathLen),
          minor, raw, headerCount,
          std::string_view(buffer).substr(static_cast<std::size_t>(parsed)),
          socket_.id());
    }
    if (parsed == -1) {
      throw HttpError(400, "malformed HTTP request", socket_.id());
    }
    previous = buffer.size();
  }
}

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
    const int fd =
        Platform::makeSocket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
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
        WaitResult result = WaitResult::Timeout;
        try {
          result = socket.wait(POLLOUT, timeout_);
        } catch (const IoFailure& e) {
          throw HttpError(500, e.what(), fd);
        }
        switch (result) {
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
          if (!iequals(trim(value), "chunked")) {
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
                              const DataHandler& onData) {
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
        decoder.feed(data, onData);
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
        onData(data.data(), n);
      }
      remaining -= n;
      return remaining == 0;
    }
    if (!data.empty()) {
      onData(data.data(), data.size());
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
                              const BodySource& body,
                              const HeadHandler& onHead,
                              const DataHandler& onData) {
  Socket socket = openConnection(request);
  transmit(socket, buildRequestHead(request));
  if (body) {
    std::string chunk;
    while (body(chunk)) {
      transmit(socket, chunk);
    }
  }
  std::string buffer;
  const ResponseHead head = readHead(socket, buffer);
  onHead(head);
  readBody(socket, head, buffer, onData);
}
