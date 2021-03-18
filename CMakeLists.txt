cmake_minimum_required(VERSION 3.10)

# set the project name
project(Experiments VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT DEFINED CONDA_HOME)
  message(FATAL_ERROR "CONDA_HOME is a required variable")
endif()

include_directories(${CONDA_HOME}/include)
include_directories(${CONDA_HOME}/include/python3.7m)
include_directories(${CONDA_HOME}/lib64/python3.7/site-packages/numpy/core/include)
link_directories(${CONDA_HOME}/lib64)
link_directories(${CONDA_HOME}/lib)

# add the executable
add_executable(read_csv read_csv.cc)
# add_executable(bench_io bench_io.cc)
# add_executable(sender sender.cc)
# add_executable(read_one_csv one_csv.cc)
# add_executable(memleak memleak.cc)

target_link_libraries(read_csv arrow)
target_link_libraries(read_csv arrow_dataset)
target_link_libraries(read_csv parquet)
target_link_libraries(read_csv aws-cpp-sdk-core)
target_link_libraries(read_csv aws-cpp-sdk-s3)
target_link_libraries(read_csv utf8proc)
target_link_libraries(read_csv lz4)
target_link_libraries(read_csv snappy)
target_link_libraries(read_csv z)
target_link_libraries(read_csv zstd)
target_link_libraries(read_csv aws-cpp-sdk-identity-management)
target_link_libraries(read_csv thrift)
# target_link_libraries(bench_io arrow)
# target_link_libraries(sender arrow)
# target_link_libraries(read_one_csv arrow)
# target_link_libraries(read_one_csv parquet)
# target_link_libraries(read_one_csv aws-cpp-sdk-core)
# target_link_libraries(read_one_csv aws-cpp-sdk-s3)
# target_link_libraries(memleak arrow)
# target_link_libraries(memleak arrow_dataset)
# target_link_libraries(memleak arrow_python)
# target_link_libraries(memleak python3.7m)
