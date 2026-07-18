#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace semzip {

std::vector<std::uint8_t> encode_varint(std::uint64_t value);
std::pair<std::uint64_t, std::size_t> decode_varint(const std::vector<std::uint8_t>& data, std::size_t cursor);

std::uint64_t zigzag_encode(std::int64_t value);
std::int64_t zigzag_decode(std::uint64_t value);

std::vector<std::uint8_t> encode_delta_values(const std::vector<std::int64_t>& values);
std::vector<std::int64_t> decode_delta_values(const std::vector<std::uint8_t>& data);

std::vector<std::uint8_t> encode_ipv4_plain(const std::vector<std::string>& tokens);
std::vector<std::string> decode_ipv4_plain(const std::vector<std::uint8_t>& data);

std::vector<std::uint8_t> encode_string_stream(const std::vector<std::string>& values);
std::vector<std::string> decode_string_stream(const std::vector<std::uint8_t>& data);

struct MtfRankResult {
    std::vector<std::uint64_t> ranks;
    std::vector<std::string> literals;
};

MtfRankResult encode_string_mtf_rank(const std::vector<std::string>& values, std::size_t table_size = 512);
std::vector<std::string> decode_string_mtf_rank(
    const std::vector<std::uint64_t>& ranks,
    const std::vector<std::string>& literals,
    std::size_t table_size = 512);

std::vector<std::uint8_t> encode_varint_stream(const std::vector<std::uint64_t>& values);
std::vector<std::uint64_t> decode_varint_stream(const std::vector<std::uint8_t>& data);

}  // namespace semzip
