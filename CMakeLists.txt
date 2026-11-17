cmake_minimum_required(VERSION 3.16)
project(http_proxy LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

add_executable(proxy
    lib/parser/picohttpparser.c
    src/main.cpp
    src/Logger.cpp
    src/StopSignal.cpp
    src/Socket.cpp
    src/Config.cpp
    src/ThreadPool.cpp
    src/Http.cpp
    src/UpstreamClient.cpp
    src/CacheBudget.cpp
    src/CacheEntry.cpp
    src/Cache.cpp
    src/Fetcher.cpp
    src/ClientSession.cpp
    src/Server.cpp
)

target_include_directories(proxy PRIVATE include lib)
target_compile_options(proxy PRIVATE -Wall -Wextra)
target_link_libraries(proxy PRIVATE Threads::Threads)
