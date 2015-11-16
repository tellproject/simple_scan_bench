cmake_minimum_required(VERSION 3.0.2)
project(simple_bench)

find_package(Crossbow REQUIRED)
find_package(Crossbow REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16 -ftemplate-depth=512")

add_executable(SimpleBench SimpleBench.cpp)
target_include_directories(SimpleBench PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(SimpleBench tellstore-client)

