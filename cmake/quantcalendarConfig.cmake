include(CMakeFindDependencyMacro)
find_dependency(fmt)
find_dependency(unordered_dense)
include("${CMAKE_CURRENT_LIST_DIR}/quantcalendar_Targets.cmake")