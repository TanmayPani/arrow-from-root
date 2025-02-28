cmake_minimum_required(VERSION 3.30)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

#Exports compilatoin flags for nvim-treesitter's lsp to find...
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(arrowjets_from_rootevents)

set(LOCAL_PREFIX_DIR "/home/tanmaypani/local" CACHE STRING "")
set(Arrow_DIR "${LOCAL_PREFIX_DIR}/lib64/cmake/Arrow" CACHE STRING "")
set(ROOT_DIR "${LOCAL_PREFIX_DIR}")

find_package(Arrow REQUIRED)
find_package(ROOT REQUIRED)

add_library(arrowjets_from_rootevents_shared SHARED)
add_subdirectory(src)

add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE arrowjets_from_rootevents_shared
                                              Arrow::arrow_shared
                                              ROOT::ROOTDataFrame ROOT::EG
                                    )
target_include_directories(${PROJECT_NAME} PRIVATE "src")
