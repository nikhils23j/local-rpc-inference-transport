cmake_minimum_required(VERSION 3.20)
project(local_rpc_inference_transport LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# Engine + admission logic: plain C++20, no gRPC, so it is unit-testable on its own.
add_library(engine src/engine.cpp)
target_include_directories(engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(engine PUBLIC Threads::Threads)
target_compile_options(engine PRIVATE -Wall -Wextra)

enable_testing()
add_executable(unit_tests tests/unit_tests.cpp)
target_link_libraries(unit_tests PRIVATE engine)
target_compile_options(unit_tests PRIVATE -Wall -Wextra)
add_test(NAME unit COMMAND unit_tests)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
set(PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/inference.proto)
set(GEN ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN})
add_custom_command(
  OUTPUT ${GEN}/inference.pb.cc ${GEN}/inference.pb.h ${GEN}/inference.grpc.pb.cc ${GEN}/inference.grpc.pb.h
  COMMAND protobuf::protoc --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto --cpp_out=${GEN}
          --grpc_out=${GEN} --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin> ${PROTO}
  DEPENDS ${PROTO}
  VERBATIM)
add_library(inference_proto ${GEN}/inference.pb.cc ${GEN}/inference.grpc.pb.cc)
target_include_directories(inference_proto PUBLIC ${GEN})
target_link_libraries(inference_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_executable(transport src/main.cpp)
target_link_libraries(transport PRIVATE engine inference_proto)
target_compile_options(transport PRIVATE -Wall -Wextra)

add_test(NAME integration COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/smoke.sh $<TARGET_FILE:transport>)
set_tests_properties(integration PROPERTIES TIMEOUT 120)
