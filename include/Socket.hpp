#pragma once

#include <chrono>
#include <cstddef>
#include <string_view>

#include "StopSignal.hpp"
#include "UniqueFd.hpp"

enum class WaitResult { Ready, Timeout, Stopped };

class Socket {
 public:
  Socket(UniqueFd fd, const StopSignal& stop) noexcept
      : fd_(std::move(fd)), stop_(&stop) {}
  Socket(Socket&&) noexcept = default;
  Socket& operator=(Socket&&) noexcept = default;
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  int id() const noexcept { return fd_.get(); }

  WaitResult wait(short events, std::chrono::milliseconds timeout) const;
  std::size_t readSome(char* buffer,
                       std::size_t size,
                       std::chrono::milliseconds timeout);
  void writeAll(std::string_view data, std::chrono::milliseconds timeout);
  void abort() noexcept;

 private:
  UniqueFd fd_;
  const StopSignal* stop_;
};
