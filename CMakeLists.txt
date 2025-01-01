cmake_minimum_required(VERSION 3.10)

project(quantcalendar VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(fmt CONFIG REQUIRED)
find_package(quantdata REQUIRED)
find_package(unordered_dense CONFIG REQUIRED)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
add_library(quantcalendar STATIC "src/calendar.cpp" "src/calendar_data.cpp")
target_link_libraries(quantcalendar
  PUBLIC quantdata::qddatetime
  PRIVATE fmt::fmt
  PRIVATE unordered_dense::unordered_dense)

# For CTest
enable_testing()

# test 
add_executable(test_calendar_data "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_calendar_data.cpp")
target_link_libraries(test_calendar_data quantcalendar quantdata::qdmongodb)
add_test(NAME "test_calendar_data" COMMAND "test_calendar_data" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
