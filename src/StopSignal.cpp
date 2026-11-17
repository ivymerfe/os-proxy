#include "StopSignal.hpp"

#include <csignal>
#include <system_error>

#include "Platform.hpp"

StopSignal::StopSignal() {
  int fds[2];
  if (::pipe(fds) < 0) {
    throw std::system_error(errno, std::generic_category(), "pipe");
  }
  UniqueFd readEnd(fds[0]);
  UniqueFd writeEnd(fds[1]);
  if (!platform::setNonBlockCloexec(fds[0]) ||
      !platform::setNonBlockCloexec(fds[1])) {
    throw std::system_error(errno, std::generic_category(), "fcntl");
  }
  readEnd_ = std::move(readEnd);
  writeEnd_ = std::move(writeEnd);
}

void StopSignal::request() noexcept {
  flag_.store(true);
  const char one = 1;
  const ssize_t written = ::write(writeEnd_.get(), &one, 1);
  (void)written;
}

std::atomic<StopSignal*> SignalHandler::target_{nullptr};

void SignalHandler::handle(int) noexcept {
  const int saved = errno;
  if (StopSignal* stop = target_.load()) {
    stop->request();
  }
  errno = saved;
}

void SignalHandler::install(StopSignal& stop) {
  target_.store(&stop);

  struct sigaction action{};
  action.sa_handler = &SignalHandler::handle;
  sigemptyset(&action.sa_mask);
  for (const int sig : {SIGINT, SIGTERM}) {
    if (::sigaction(sig, &action, nullptr) < 0) {
      throw std::system_error(errno, std::generic_category(), "sigaction");
    }
  }

  struct sigaction ignore{};
  ignore.sa_handler = SIG_IGN;
  sigemptyset(&ignore.sa_mask);
  if (::sigaction(SIGPIPE, &ignore, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "sigaction(SIGPIPE)");
  }
}
