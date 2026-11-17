#pragma once

#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

class HttpError : public std::runtime_error {
 public:
  HttpError(int status, const std::string& message, int socketId = -1)
      : std::runtime_error(message), status_(status), socketId_(socketId) {}

  int status() const noexcept { return status_; }
  int socketId() const noexcept { return socketId_; }

 private:
  int status_;
  int socketId_;
};

struct IoTimeout : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct IoStopped : std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct IoFailure : std::runtime_error {
  IoFailure(const std::string& message, int code)
      : std::runtime_error(message), code_(code) {}
  int code() const noexcept { return code_; }

 private:
  int code_;
};

inline std::string describeErrno(std::string_view what, int err) {
  std::string result(what);
  result += ": ";
  result += std::system_category().message(err);
  return result;
}
