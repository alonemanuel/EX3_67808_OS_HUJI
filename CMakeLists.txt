cmake_minimum_required(VERSION 3.12)
project(Ex3)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(Ex3 MapReduceFramework.h MapReduceFramework.cpp MapReduceClient.h SampleClient.cpp Barrier.cpp Barrier.h
        Tests/Tarantino/WordFrequencies.cpp Tests/Tarantino/WordFrequenciesClient.hpp)