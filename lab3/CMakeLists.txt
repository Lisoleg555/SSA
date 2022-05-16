cmake_minimum_required(VERSION 3.2)

project(LR3 C CXX)

find_package(OpenSSL)
find_package(GTest REQUIRED)
find_package(Threads)
find_package(Poco REQUIRED COMPONENTS Foundation Util Net XML JSON)
find_package(Java 1.8 REQUIRED)
find_package(JNI REQUIRED)

if(NOT ${Poco_FOUND})
    message(FATAL_ERROR "Poco C++ Libraries not found.")
endif()

include_directories(${Boost_INCLUDE_DIR})
include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${Poco_INCLUDE_DIRS})
include_directories(SYSTEM ${IGNITE_INCLUDE_DIR} ${JNI_INCLUDE_DIRS})
include_directories("/usr/include/mysql")
include_directories("/usr/local/include/mysql")

link_directories("/usr/local/lib")

add_executable(pocoserver pocoserver.cpp)
add_executable(googletest googletest.cpp)

target_include_directories(pocoserver PRIVATE "${CMAKE_BINARY_DIR}")
target_include_directories(googletest PRIVATE "${CMAKE_BINARY_DIR}")
target_compile_options(pocoserver PRIVATE)
target_compile_options(googletest PRIVATE)
target_link_libraries(pocoserver PRIVATE 
                      ${CMAKE_THREAD_LIBS_INIT} 
                      ${Poco_LIBRARIES}
                      "PocoData"
                      "PocoDataMySQL"
                      "ignite-binary"
                      "ignite-common"
                      ${IGNITE_JNI_LIB}
                      "ignite-thin-client"
                      "mysqlclient")
target_link_libraries(googletest PRIVATE 
                      ${CMAKE_THREAD_LIBS_INIT} 
                      ${Poco_LIBRARIES}
                      ${GTEST_LIBRARIES}
                      "PocoData"
                      "PocoDataMySQL"
                      "ignite-binary"
                      "ignite-common"
                      ${IGNITE_JNI_LIB}
                      "ignite-thin-client"
                      "mysqlclient")
set_target_properties(pocoserver PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(googletest PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(pocoserver PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
set_target_properties(googletest PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
