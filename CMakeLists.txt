# PythonExtensions depends on FindPythonInterp and
# FindPythonLibs modules which are removed since CMake 3.27.
# WARNING: vcpkg will override VERSION
cmake_minimum_required(VERSION 3.15...3.26)

# C Language is a must. Because some link tests will be done on Macos
# to use dynamic lookup for buiding python modules.
# Weak link test that need c compiler cannot be skipped.
# @see https://blog.tim-smith.us/2015/09/python-extension-modules-os-x/
project(quantcalendar VERSION 1.0 LANGUAGES C CXX)

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

  # Generate pxi template class
  set(CALENDAR_DATA_CLASS "DatesArray")
  set(DEFAULT_COUNT "=2**32-1")
  configure_file("quantcalendar/_quantcalendar.pxi.in" "${CMAKE_CURRENT_SOURCE_DIR}/quantcalendar/_quantcalendar_${CALENDAR_DATA_CLASS}.pxi")
  set(CALENDAR_DATA_CLASS "Date7x24Array")
  set(DEFAULT_COUNT "")
  configure_file("quantcalendar/_quantcalendar.pxi.in" "${CMAKE_CURRENT_SOURCE_DIR}/quantcalendar/_quantcalendar_${CALENDAR_DATA_CLASS}.pxi")

  add_cython_target(_quantcalendar "quantcalendar/_quantcalendar.pyx" CXX)
  add_library(_quantcalendar MODULE "src/calendar.cpp" "src/dates.cpp" ${_quantcalendar})
  target_link_libraries(_quantcalendar
    quantdata::qddatetime
    fmt::fmt
    unordered_dense::unordered_dense)
  python_extension_module(_quantcalendar)

  install(TARGETS _quantcalendar LIBRARY DESTINATION quantcalendar)
  install(FILES
    "quantcalendar/__init__.py"
    "quantcalendar/_quantcalendar.pxd"
    "quantcalendar/_quantcalendar.pyi"
    DESTINATION quantcalendar)
else()
  add_library(quantcalendar STATIC "src/calendar.cpp" "src/dates.cpp")
  target_link_libraries(quantcalendar
    quantdata::qddatetime
    fmt::fmt
    unordered_dense::unordered_dense)

  # For CTest
  enable_testing()

  # test 
  add_executable(test_calendar_data "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_calendar_data.cpp")
  target_link_libraries(test_calendar_data quantcalendar quantdata::qdmongodb)
  add_test(NAME "test_calendar_data" COMMAND "test_calendar_data" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
endif()
