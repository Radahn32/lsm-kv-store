cmake_minimum_required(VERSION 3.14)
project(lsm_kv_store LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optional AddressSanitizer + UndefinedBehaviorSanitizer for catching memory bugs early
option(ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)
if(ENABLE_SANITIZERS)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
endif()

# Fetch GoogleTest automatically
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

enable_testing()
include(GoogleTest)

# Core library with auto-discovered sources
file(GLOB_RECURSE KVSTORE_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
list(FILTER KVSTORE_SOURCES EXCLUDE REGEX ".*/main\\.cpp$")

add_library(kvstore_core ${KVSTORE_SOURCES})
target_include_directories(kvstore_core PUBLIC include)

add_executable(kvstore_cli src/cli/main.cpp)
target_link_libraries(kvstore_cli PRIVATE kvstore_core)

add_executable(kvstore_bench src/bench/main.cpp)
target_link_libraries(kvstore_bench PRIVATE kvstore_core)

# Auto-discover test executables
file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tests/*_test.cpp")

foreach(test_src ${TEST_SOURCES})
    get_filename_component(test_target ${test_src} NAME_WE)
    add_executable(${test_target} ${test_src})
    target_include_directories(${test_target} PRIVATE include)
    target_link_libraries(${test_target} PRIVATE kvstore_core GTest::gtest_main)
    if(ENABLE_SANITIZERS)
        gtest_discover_tests(${test_target} PROPERTIES ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0")
    else()
        gtest_discover_tests(${test_target})
    endif()
endforeach()
