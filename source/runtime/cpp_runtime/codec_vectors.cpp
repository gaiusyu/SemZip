#include "semzip_codecs.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string hex_bytes(const std::vector<std::uint8_t>& data) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (std::uint8_t byte : data) {
        out << std::setw(2) << static_cast<int>(byte);
    }
    return out.str();
}

}  // namespace

int main() {
    using namespace semzip;
    const std::vector<std::int64_t> numbers = {100, 101, 99, 105, -3, -3, 5000};
    const std::vector<std::string> ips = {"10.0.0.1", "173.234.31.186", "255.255.255.255"};
    const std::vector<std::string> strings = {"alpha", "beta", "alpha", "gamma", "beta", "alpha"};

    const auto mtf = encode_string_mtf_rank(strings);
    std::cout << "delta_hex=" << hex_bytes(encode_delta_values(numbers)) << "\n";
    std::cout << "ipv4_hex=" << hex_bytes(encode_ipv4_plain(ips)) << "\n";
    std::cout << "string_stream_hex=" << hex_bytes(encode_string_stream(strings)) << "\n";
    std::cout << "mtf_rank_hex=" << hex_bytes(encode_varint_stream(mtf.ranks)) << "\n";
    std::cout << "mtf_literal_hex=" << hex_bytes(encode_string_stream(mtf.literals)) << "\n";
    return 0;
}
