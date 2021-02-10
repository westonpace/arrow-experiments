cmake_minimum_required(VERSION 3.10)

# set the project name
project(Experiments VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(/home/pace/anaconda3/envs/conbench2/include)
link_directories(/home/pace/anaconda3/envs/conbench2/lib)

# add the executable
add_executable(read_csv read_csv.cc)
add_executable(bench_io bench_io.cc)

target_link_libraries(read_csv arrow)
target_link_libraries(bench_io arrow)