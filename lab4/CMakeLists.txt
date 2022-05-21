cmake_minimum_required(VERSION 3.2)

project(LR4 C CXX)

find_package(OpenSSL)
find_package(GTest REQUIRED)
find_package(Threads)
find_package(Poco REQUIRED COMPONENTS Foundation Util Net XML JSON Crypto NetSSL)
find_package(CppKafka REQUIRED)

if(NOT ${Poco_FOUND})
    message(FATAL_ERROR "Poco C++ Libraries not found.")
endif()

include_directories(${Boost_INCLUDE_DIR})
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${Poco_INCLUDE_DIRS})
include_directories("/usr/include/mysql")
include_directories("/usr/local/include/mysql")

link_directories("/usr/local/lib")

add_executable(pocoserver pocoserver.cpp)
add_executable(googletest googletest.cpp)
add_executable(kafka kafka.cpp)

target_include_directories(pocoserver PRIVATE "${CMAKE_BINARY_DIR}")
target_include_directories(googletest PRIVATE "${CMAKE_BINARY_DIR}")
target_include_directories(kafka PRIVATE "${CMAKE_BINARY_DIR}")
target_compile_options(pocoserver PRIVATE)
target_compile_options(googletest PRIVATE)
target_compile_options(kafka PRIVATE)
target_link_libraries(pocoserver PRIVATE 
                      ${CMAKE_THREAD_LIBS_INIT} 
                      ${Poco_LIBRARIES}
                      "PocoData"
                      "PocoDataMySQL"
                      "rdkafka++"
                      CppKafka::cppkafka
                      "mysqlclient")
target_link_libraries(googletest PRIVATE 
                      ${CMAKE_THREAD_LIBS_INIT} 
                      ${Poco_LIBRARIES}
                      ${GTEST_LIBRARIES}
                      "PocoData"
                      "PocoDataMySQL"
                      "rdkafka++"
                      CppKafka::cppkafka
                      "mysqlclient")
target_link_libraries(kafka PRIVATE 
                      ${CMAKE_THREAD_LIBS_INIT} 
                      ${Poco_LIBRARIES}
                      ${GTEST_LIBRARIES}
                      "PocoData"
                      "PocoDataMySQL"
                      "rdkafka++"
                      CppKafka::cppkafka
                      "mysqlclient")
