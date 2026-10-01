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

void write_bytes(const std::string& path, const std::vector<std::uint8_t>& data) {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("cannot open output: " + path);
    }
    output.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!output) {
        throw std::runtime_error("failed writing output: " + path);
    }
}

std::vector<std::int64_t> parse_int_values(const std::vector<std::string>& values) {
    std::vector<std::int64_t> parsed;
    parsed.reserve(values.size());
    for (const auto& value : values) {
        std::size_t cursor = 0;
        long long parsed_value = std::stoll(value, &cursor, 10);
        if (cursor != value.size()) {
            throw std::runtime_error("non-integer value for delta stream: " + value);
        }
        parsed.push_back(static_cast<std::int64_t>(parsed_value));
    }
    return parsed;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 4) {
            std::cerr << "Usage:\n"
                      << "  stream_writer delta <values.strings.bin> <out.delta.bin>\n"
                      << "  stream_writer ipv4_plain <values.strings.bin> <out.ipv4.bin>\n"
                      << "  stream_writer string_mtf_rank <values.strings.bin> <out.rank.bin> <out.literal.bin>\n";
            return 2;
        }

        const std::string kind = argv[1];
        const auto values = semzip::decode_string_stream(read_bytes(argv[2]));

        if (kind == "delta") {
            if (argc != 4) {
                throw std::runtime_error("delta expects exactly one output file");
            }
            write_bytes(argv[3], semzip::encode_delta_values(parse_int_values(values)));
            std::cout << "WROTE delta count=" << values.size() << "\n";
            return 0;
        }

        if (kind == "ipv4_plain") {
            if (argc != 4) {
                throw std::runtime_error("ipv4_plain expects exactly one output file");
            }
            write_bytes(argv[3], semzip::encode_ipv4_plain(values));
            std::cout << "WROTE ipv4_plain count=" << values.size() << "\n";
            return 0;
        }

        if (kind == "string_mtf_rank") {
            if (argc != 5) {
                throw std::runtime_error("string_mtf_rank expects rank and literal output files");
            }
            const auto encoded = semzip::encode_string_mtf_rank(values);
            write_bytes(argv[3], semzip::encode_varint_stream(encoded.ranks));
            write_bytes(argv[4], semzip::encode_string_stream(encoded.literals));
            std::cout << "WROTE string_mtf_rank count=" << values.size() << "\n";
            return 0;
        }

        throw std::runtime_error("unsupported kind: " + kind);
    } catch (const std::exception& exc) {
        std::cerr << "FAIL " << exc.what() << "\n";
        return 1;
    }
}
