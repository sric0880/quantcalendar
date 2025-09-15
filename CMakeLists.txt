# PythonExtensions depends on FindPythonInterp and
# FindPythonLibs modules which are removed since CMake 3.27.
# WARNING: vcpkg will override VERSION
cmake_minimum_required(VERSION 3.15...3.26)

# C Language is a must. Because some link tests will be done on Macos
# to use dynamic lookup for buiding python modules.
# Weak link test that need c compiler cannot be skipped.
# @see https://blog.tim-smith.us/2015/09/python-extension-modules-os-x/
project(quantcalendar VERSION 2025.09 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

# use hints for not using lib under anaconda
find_package(unordered_dense REQUIRED)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
set(SOURCES
  "src/calendar.cpp"
  "src/dates.cpp"
  "src/datetime.cpp"
  "src/exceptions.cpp"
  "src/calendar_data.cpp"
  "src/init.cpp")

# We are using the SKBUILD variable, which is defined when scikit-build is
# running the CMake build, to control building the Python wrapper. This allows
# the C++ project to be installed, standalone, when using the standard CMake
# build flow.
if(DEFINED SKBUILD)
  message(STATUS "The project is built using scikit-build")
  # PythonExtensions @see https://scikit-build.readthedocs.io/en/latest/cmake-modules/PythonExtensions.html
  find_package(PythonExtensions REQUIRED)
  # NumPy @see https://scikit-build.readthedocs.io/en/latest/cmake-modules/NumPy.html
  find_package(NumPy REQUIRED)
  find_package(Cython REQUIRED)

  include_directories(${NumPy_INCLUDE_DIRS})
  # Generate pxi template class
  set(CALENDAR_DATA_CLASS "DatesArray")
  set(DEFAULT_COUNT "=2**32-1")
  configure_file("quantcalendar/_quantcalendar.pxi.in" "${CMAKE_CURRENT_SOURCE_DIR}/quantcalendar/_quantcalendar_${CALENDAR_DATA_CLASS}.pxi")
  set(CALENDAR_DATA_CLASS "Date7x24Array")
  set(DEFAULT_COUNT "")
  configure_file("quantcalendar/_quantcalendar.pxi.in" "${CMAKE_CURRENT_SOURCE_DIR}/quantcalendar/_quantcalendar_${CALENDAR_DATA_CLASS}.pxi")

  add_cython_target(_quantcalendar "quantcalendar/_quantcalendar.pyx" CXX)
  add_compile_definitions(NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION)
  add_library(_quantcalendar MODULE ${SOURCES} ${_quantcalendar})
  target_link_libraries(_quantcalendar
    unordered_dense::unordered_dense)
  python_extension_module(_quantcalendar)

  install(TARGETS _quantcalendar LIBRARY DESTINATION quantcalendar)
  install(FILES
    "quantcalendar/__init__.py"
    "quantcalendar/_quantcalendar.pxd"
    "quantcalendar/_quantcalendar.pyi"
    DESTINATION quantcalendar)
else()
  add_library(quantcalendar SHARED ${SOURCES})
  target_link_libraries(quantcalendar
    PUBLIC unordered_dense::unordered_dense)

  # For CTest
  include(CTest)
  if (BUILD_TESTING)
      # test 
      add_executable(test_calendar_data "${CMAKE_CURRENT_SOURCE_DIR}/tests/test_calendar_data.cpp")
      target_link_libraries(test_calendar_data quantcalendar)
      add_test(NAME "test_calendar_data" COMMAND "test_calendar_data" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

  # For Install
  # example from https://cmake.org/cmake/help/latest/manual/cmake-packages.7.html#config-file-packages
  install(
    TARGETS quantcalendar
    EXPORT quantcalendar_Targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include)

  include(CMakePackageConfigHelpers)
  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/quantcalendarConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
  )
  export(EXPORT quantcalendar_Targets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cmake/quantcalendarTargets.cmake"
    NAMESPACE quantcalendar::
  )
  set(ConfigPackageLocation lib/cmake/quantcalendar)
  install(EXPORT quantcalendar_Targets
    FILE
    quantcalendar_Targets.cmake
    NAMESPACE
    quantcalendar::
    DESTINATION
    ${ConfigPackageLocation}
  )
  install(
    FILES
    cmake/quantcalendarConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/quantcalendarConfigVersion.cmake"
    DESTINATION
    ${ConfigPackageLocation}
  )
  install(
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/quantcalendar"
    DESTINATION include)
endif()
