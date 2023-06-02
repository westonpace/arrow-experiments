cmake_minimum_required(VERSION 3.10)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
# set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# set the project name
project(Experiments VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT DEFINED CONDA_HOME)
  if(DEFINED ENV{CONDA_PREFIX})
    set(CONDA_HOME "$ENV{CONDA_PREFIX}")
  else()
    message(FATAL_ERROR "CONDA_HOME is a required variable")
  endif()
endif()

message(STATUS "Using CONDA_HOME=${CONDA_HOME}")

include_directories(SYSTEM ${CONDA_HOME}/include)
link_directories(${CONDA_HOME}/lib64)
link_directories(${CONDA_HOME}/lib)

function(experiment TARGET)
    add_executable(
            ${TARGET}
            ${TARGET}.cc
    )
    target_link_libraries(
            ${TARGET}
            arrow
            arrow_acero
            arrow_dataset
            parquet
            aws-cpp-sdk-core
            aws-cpp-sdk-s3
            glog
            pthread
            re2
            utf8proc
            lz4
            snappy
            z
            zstd
            orc
            aws-cpp-sdk-identity-management
            thrift
    )
    if (MSVC)
        target_compile_options(${TARGET} PRIVATE /W4 /WX)
    else ()
        target_compile_options(${TARGET} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif ()
endfunction()

experiment(arrow_16642)
experiment(example)

# add the executable
add_executable(read_one_csv one_csv.cc)
add_executable(tpch_repro tpch_repro.cc)

# find_package(PAPI REQUIRED)

target_link_libraries(tpch_repro arrow)
target_link_libraries(tpch_repro arrow_dataset)
target_link_libraries(tpch_repro parquet)
target_link_libraries(tpch_repro aws-cpp-sdk-core)
target_link_libraries(tpch_repro aws-cpp-sdk-s3)
target_link_libraries(tpch_repro glog)
target_link_libraries(tpch_repro pthread)
target_link_libraries(tpch_repro re2)
target_link_libraries(tpch_repro utf8proc)
target_link_libraries(tpch_repro lz4)
target_link_libraries(tpch_repro snappy)
target_link_libraries(tpch_repro z)
target_link_libraries(tpch_repro zstd)
target_link_libraries(tpch_repro aws-cpp-sdk-identity-management)
target_link_libraries(tpch_repro thrift)
target_link_libraries(read_one_csv arrow)
# target_link_libraries(read_one_csv parquet)
# target_link_libraries(read_one_csv aws-cpp-sdk-core)
# target_link_libraries(read_one_csv aws-cpp-sdk-s3)
