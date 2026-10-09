cmake_minimum_required(VERSION 3.10)
project(LogAgent)

set(CMAKE_CXX_STANDARD 11)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

include_directories(${RDKAFKA_INCLUDE_DIRS})
link_directories(${RDKAFKA_LIBRARY_DIRS})

add_executable(agent agent.cpp)
target_link_libraries(agent ${RDKAFKA_LIBRARIES})
