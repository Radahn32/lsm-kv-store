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

# Sanity test to verify toolchain
add_executable(sanity_test tests/sanity_test.cpp)
target_include_directories(sanity_test PRIVATE include)
target_link_libraries(sanity_test PRIVATE GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(sanity_test)
