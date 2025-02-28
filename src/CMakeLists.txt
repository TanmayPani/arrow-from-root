target_sources(arrowjets_from_rootevents_shared PRIVATE ${CMAKE_CURRENT_LIST_DIR}/arrowjets-from-rootevents.cpp 
                                                 ${CMAKE_CURRENT_LIST_DIR}/arrowjets-from-rootevents.hh
                )

set(FASTJET_LIB_DIR "${LOCAL_PREFIX_DIR}/lib" CACHE STRING "")
set(FASTJET_INCLUDE_DIR "${LOCAL_PREFIX_DIR}/include" CACHE STRING "")

include(CMakePrintHelpers)
cmake_print_variables(ROOT_DIR Arrow_DIR FASTJET_LIB_DIR FASTJET_INCLUDE_DIR)
cmake_print_variables(CMAKE_CURRENT_LIST_DIR CMAKE_BINARY_DIR)

find_library(FASTJET_LIB NAMES fastjet PATHS "${FASTJET_LIB_DIR}" NO_DEFAULT_PATH REQUIRED)
if(NOT FASTJET_LIB)
    message(FATAL_ERROR "FastJet library not found")
endif()

separate_arguments(ROOT_EXE_LINKER_FLAGS)
target_link_libraries(arrowjets_from_rootevents_shared PRIVATE Arrow::arrow_shared ROOT::ROOTDataFrame ROOT::EG ${FASTJET_LIB})

target_include_directories(arrowjets_from_rootevents_shared PRIVATE ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(arrowjets_from_rootevents_shared PRIVATE "${CMAKE_CURRENT_LIST_DIR}/../include")
target_include_directories(arrowjets_from_rootevents_shared PUBLIC ${FASTJET_INCLUDE_DIR})