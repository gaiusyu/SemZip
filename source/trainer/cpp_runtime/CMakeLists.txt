cmake_minimum_required(VERSION 3.16)
project(semzip_cpp_runtime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(semzip_codecs semzip_codecs.cpp)
target_include_directories(semzip_codecs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(codec_smoke codec_smoke.cpp)
target_link_libraries(codec_smoke PRIVATE semzip_codecs)

add_executable(codec_vectors codec_vectors.cpp)
target_link_libraries(codec_vectors PRIVATE semzip_codecs)

add_executable(stream_roundtrip stream_roundtrip.cpp)
target_link_libraries(stream_roundtrip PRIVATE semzip_codecs)

add_executable(stream_writer stream_writer.cpp)
target_link_libraries(stream_writer PRIVATE semzip_codecs)

add_executable(stream_writer_server stream_writer_server.cpp)
target_link_libraries(stream_writer_server PRIVATE semzip_codecs)

find_library(PCRE2_8_LIBRARY pcre2-8)
if(PCRE2_8_LIBRARY)
  add_executable(pcre2_match_dump pcre2_match_dump.cpp)
  target_link_libraries(pcre2_match_dump PRIVATE ${PCRE2_8_LIBRARY})
  add_executable(pcre2_replay_dump pcre2_replay_dump.cpp)
  target_link_libraries(pcre2_replay_dump PRIVATE ${PCRE2_8_LIBRARY})
endif()
