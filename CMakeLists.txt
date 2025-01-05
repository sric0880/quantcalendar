cmake_minimum_required(VERSION 3.15...3.26)

project(quantcalendar VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(fmt CONFIG REQUIRED)
find_package(unordered_dense REQUIRED)
find_package(quantdata REQUIRED)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")

# We are using the SKBUILD variable, which is defined when scikit-build is
# running the CMake build, to control building the Python wrapper. This allows
# the C++ project to be installed, standalone, when using the standard CMake
# build flow.
if(DEFINED SKBUILD)
  message(STATUS "The project is built using scikit-build")
  # PythonExtensions @see https://scikit-build.readthedocs.io/en/latest/cmake-modules/PythonExtensions.html
  find_package(PythonExtensions REQUIRED)
  find_package(Cython REQUIRED)

  add_cython_target(calendar "quantcalendar/calendar.pyx" CXX)
  add_library(calendar MODULE ${calendar})
  target_link_libraries(calendar
    quantdata::qddatetime
    fmt::fmt
    unordered_dense::unordered_dense)
  python_extension_module(calendar)

  install(TARGETS calendar LIBRARY DESTINATION quantcalendar)
  install(FILES
    "quantcalendar/__init__.py"
    "quantcalendar/calendar.pxd"
    "quantcalendar/calendar.pyi"
    DESTINATION quantcalendar)
else()
  add_library(quantcalendar STATIC "src/calendar.cpp" "src/calendar_data.cpp")
  target_link_libraries(quantcalendar
    PUBLIC quantdata::qddatetime
    PUBLIC fmt::fmt
    PUBLIC unordered_dense::unordered_dense)

  # For CTest
  enable_testing()

  # test 
  add_executable(test_calendar_data "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_calendar_data.cpp")
  target_link_libraries(test_calendar_data quantcalendar quantdata::qdmongodb)
  add_test(NAME "test_calendar_data" COMMAND "test_calendar_data" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
endif()
