#include "Http.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>

#include "Errors.hpp"
#include "parser/picohttpparser.h"

namespace {

constexpr std::size_t kMaxHeaders = 100;

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

void splitAuthority(std::string_view authority,
                    std::string& host,
                    std::string& port,
                    int socketId) {
  port = "80";
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
  if (target.substr(0, 7) == "http://") {
    const auto rest2 = target.substr(7);
    const auto split = rest2.find_first_of("/?");
    authority.assign(rest2.substr(0, split));
    if (split == std::string_view::npos) {
      request.path = "/";
    } else {
      request.path.assign(rest2.substr(split));
      if (request.path.front() == '?') {
        request.path.insert(request.path.begin(), '/');
      }
    }
  } else if (target.substr(0, 8) == "https://") {
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
