cmake_minimum_required(VERSION 3.10)

project(quantcalendar CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(fmt CONFIG REQUIRED)
find_package(quantdata REQUIRED)
find_package(unordered_dense CONFIG REQUIRED)

add_library(quantcalendar STATIC "src/calendar.cpp")

include_directories("include")

target_link_libraries(quantcalendar
  PRIVATE fmt::fmt
  PRIVATE quantdata::qddatetime
  PRIVATE unordered_dense::unordered_dense)

# For CTest
enable_testing()

# test 
add_executable(test_test "${CMAKE_CURRENT_SOURCE_DIR}/tests/test.cpp")
target_link_libraries(test_test quantdata::qddatetime)
add_test(NAME "test_test" COMMAND "test_test" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
