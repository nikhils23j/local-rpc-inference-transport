cmake_minimum_required(VERSION 3.20)
project(local_rpc_inference_transport LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)
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
target_link_libraries(transport PRIVATE inference_proto Threads::Threads)
enable_testing()
add_test(NAME smoke COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/smoke.sh $<TARGET_FILE:transport>)
