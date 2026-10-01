#include "semzip_codecs.hpp"

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::vector<std::uint8_t> read_bytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open input: " + path);
    }
    return std::vector<std::uint8_t>(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>());
}

void expect_equal(const std::vector<std::uint8_t>& actual, const std::vector<std::uint8_t>& expected, const std::string& label) {
    if (actual != expected) {
        throw std::runtime_error(label + " byte roundtrip mismatch");
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 3) {
            std::cerr << "Usage: stream_roundtrip <kind> <file> [literal_file]\n";
            return 2;
        }
        const std::string kind = argv[1];
        const std::string file = argv[2];

        if (kind == "delta") {
            const auto original = read_bytes(file);
            const auto values = semzip::decode_delta_values(original);
            expect_equal(semzip::encode_delta_values(values), original, "delta");
            std::cout << "PASS delta count=" << values.size() << "\n";
            return 0;
        }

        if (kind == "ipv4_plain") {
            const auto original = read_bytes(file);
            const auto values = semzip::decode_ipv4_plain(original);
            expect_equal(semzip::encode_ipv4_plain(values), original, "ipv4_plain");
            std::cout << "PASS ipv4_plain count=" << values.size() << "\n";
            return 0;
        }

        if (kind == "string") {
            const auto original = read_bytes(file);
            const auto values = semzip::decode_string_stream(original);
            expect_equal(semzip::encode_string_stream(values), original, "string");
            std::cout << "PASS string count=" << values.size() << "\n";
            return 0;
        }

        if (kind == "string_mtf_rank") {
            if (argc < 4) {
                throw std::runtime_error("string_mtf_rank requires rank_file and literal_file");
            }
            const auto rank_original = read_bytes(file);
            const auto literal_original = read_bytes(argv[3]);
            const auto ranks = semzip::decode_varint_stream(rank_original);
            const auto literals = semzip::decode_string_stream(literal_original);
            const auto values = semzip::decode_string_mtf_rank(ranks, literals);
            const auto encoded = semzip::encode_string_mtf_rank(values);
            expect_equal(semzip::encode_varint_stream(encoded.ranks), rank_original, "string_mtf_rank ranks");
            expect_equal(semzip::encode_string_stream(encoded.literals), literal_original, "string_mtf_rank literals");
            std::cout << "PASS string_mtf_rank count=" << values.size() << "\n";
            return 0;
        }

        throw std::runtime_error("unsupported kind: " + kind);
    } catch (const std::exception& exc) {
        std::cerr << "FAIL " << exc.what() << "\n";
        return 1;
    }
}
