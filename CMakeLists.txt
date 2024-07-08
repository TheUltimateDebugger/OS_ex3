cmake_minimum_required(VERSION 3.10)
project(ex3)

set(CMAKE_CXX_STANDARD 11)

# Include directories
include_directories(${CMAKE_SOURCE_DIR})

# Specify the source files for the Barrier
set(BARRIER_SOURCES
        Barrier/Barrier.cpp
        )

# Specify the source files for the MapReduceFramework
set(MAPREDUCE_SOURCES
        MapReduceFramework.cpp
        )

# Create the Barrier static library
add_library(Barrier STATIC ${BARRIER_SOURCES})

# Create the MapReduceFramework static library and link Barrier
add_library(MapReduceFramework STATIC ${MAPREDUCE_SOURCES})
target_link_libraries(MapReduceFramework Barrier)

# Specify the source files for the SampleClient executable
set(SAMPLE_CLIENT_SOURCES
        SampleClient/SampleClient.cpp
        )

# Create the SampleClient executable and link MapReduceFramework
add_executable(SampleClient ${SAMPLE_CLIENT_SOURCES})
target_link_libraries(SampleClient MapReduceFramework)