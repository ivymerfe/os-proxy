#pragma once

#include <cstddef>

#include "Http.hpp"

class ResponseSink {
 public:
  virtual ~ResponseSink() = default;
  virtual void onHead(const ResponseHead& head) = 0;
  virtual void onBody(const char* data, std::size_t size) = 0;
  virtual void onComplete() = 0;
};

class RequestBodySource {
 public:
  virtual ~RequestBodySource() = default;
  virtual bool next(std::string& out) = 0;
};
