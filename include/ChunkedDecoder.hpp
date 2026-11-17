#pragma once

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

class ChunkedDecoder {
 public:
  bool done() const noexcept { return state_ == State::Done; }

  template <typename Emit>
  void feed(std::string_view input, Emit&& emit) {
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
