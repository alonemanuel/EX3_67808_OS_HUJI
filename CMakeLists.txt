cmake_minimum_required(VERSION 3.12)
project(ex3-67808-os-huji)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(ex3 MapReduceFramework.h MapReduceFramework.cpp SampleClient.cpp
        Barrier.cpp)