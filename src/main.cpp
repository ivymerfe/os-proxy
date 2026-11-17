#include <cstdio>
#include <exception>
#include <iostream>
#include <stdexcept>

#include "Config.hpp"
#include "Logger.hpp"
#include "Server.hpp"
#include "StopSignal.hpp"

int main(int argc, char** argv) {
  try {
    const auto config = ConfigParser::parse(argc, argv);
    if (!config) {
      std::cout << ConfigParser::usage(argv[0]);
      return 0;
    }
    Logger::instance().setLevel(config->logLevel);
    StopSignal stop;
    SignalHandler::install(stop);
    Server server(*config, stop);
    return server.run();
  } catch (const std::invalid_argument& e) {
    std::fprintf(stderr, "error: %s\n\n%s", e.what(),
                 ConfigParser::usage(argv[0]).c_str());
    return 2;
  } catch (const std::exception& e) {
    Logger::instance().print(LogLevel::Error, "startup failed: ", e.what());
    return 1;
  }
}
