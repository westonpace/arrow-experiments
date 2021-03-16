cmake_minimum_required(VERSION 3.10)

# set the project name
project(Experiments VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT DEFINED CONDA_ENV)
  message(FATAL_ERROR "CONDA_ENV is a required variable")
endif()

include_directories(/home/pace/anaconda3/envs/${CONDA_ENV}/include)
include_directories(/home/pace/anaconda3/envs/${CONDA_ENV}/include/python3.7m)
include_directories(/home/pace/anaconda3/envs/${CONDA_ENV}/lib/python3.7/site-packages/numpy/core/include)
link_directories(/home/pace/anaconda3/envs/${CONDA_ENV}/lib)

# add the executable
add_executable(read_csv read_csv.cc)
add_executable(bench_io bench_io.cc)
add_executable(sender sender.cc)
add_executable(read_one_csv one_csv.cc)
# add_executable(memleak memleak.cc)

target_link_libraries(read_csv arrow)
target_link_libraries(read_csv arrow_dataset)
target_link_libraries(bench_io arrow)
target_link_libraries(sender arrow)
target_link_libraries(read_one_csv arrow)
# target_link_libraries(memleak arrow)
# target_link_libraries(memleak arrow_dataset)
# target_link_libraries(memleak arrow_python)
# target_link_libraries(memleak python3.7m)
