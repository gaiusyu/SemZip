#include "semzip_codecs.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

template <typename T>
void require_equal(const std::vector<T>& got, const std::vector<T>& expected, const std::string& name) {
    if (got != expected) {
        throw std::runtime_error("mismatch in " + name);
    }
}

}  // namespace

int main() {
    using namespace semzip;

    const std::vector<std::int64_t> numbers = {100, 101, 99, 105, -3, -3, 5000};
    require_equal(decode_delta_values(encode_delta_values(numbers)), numbers, "delta");

    const std::vector<std::string> ips = {"10.0.0.1", "173.234.31.186", "255.255.255.255"};
    require_equal(decode_ipv4_plain(encode_ipv4_plain(ips)), ips, "ipv4_plain");

    bool rejected_leading_zero = false;
    try {
        (void)encode_ipv4_plain({"010.0.0.1"});
    } catch (const std::exception&) {
        rejected_leading_zero = true;
    }
    if (!rejected_leading_zero) {
        throw std::runtime_error("ipv4_plain accepted leading-zero IPv4");
    }

    const std::vector<std::string> strings = {"alpha", "beta", "alpha", "gamma", "beta", "alpha"};
    const auto mtf = encode_string_mtf_rank(strings);
    require_equal(decode_string_mtf_rank(mtf.ranks, mtf.literals), strings, "string_mtf_rank");
    require_equal(decode_string_stream(encode_string_stream(strings)), strings, "string_stream");

    const std::vector<std::uint64_t> ranks = {0, 1, 127, 128, 16384};
    require_equal(decode_varint_stream(encode_varint_stream(ranks)), ranks, "varint_stream");

    std::cout << "codec_smoke PASS\n";
    return 0;
}
