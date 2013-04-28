cmake_minimum_required(VERSION 2.8)
project(external_sort)

##################
# Compiler setting
##################

# Use C++ 11
set(CMAKE_CXX_FLAGS "-Wall -g -std=c++11")
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")

########################
# Build executable files
########################
set(EXECUTABLE_OUTPUT_PATH "${PROJECT_BINARY_DIR}/bin")
add_executable(sort sort.cpp external_sort.cpp external_sort.h)

