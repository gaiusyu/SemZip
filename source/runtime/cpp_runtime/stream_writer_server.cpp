#include "semzip_codecs.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
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

std::vector<std::string> split_tab(const std::string& line) {
    std::vector<std::string> parts;
    std::string item;
    std::stringstream stream(line);
    while (std::getline(stream, item, '\t')) {
        parts.push_back(item);
    }
    return parts;
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

void handle_write(const std::vector<std::string>& parts) {
    if (parts.size() < 4 || parts[0] != "WRITE") {
        throw std::runtime_error("bad command");
    }
    const std::string& kind = parts[1];
    const auto values = semzip::decode_string_stream(read_bytes(parts[2]));

    if (kind == "delta") {
        if (parts.size() != 4) {
            throw std::runtime_error("delta expects one output file");
        }
        write_bytes(parts[3], semzip::encode_delta_values(parse_int_values(values)));
        return;
    }

    if (kind == "ipv4_plain") {
        if (parts.size() != 4) {
            throw std::runtime_error("ipv4_plain expects one output file");
        }
        write_bytes(parts[3], semzip::encode_ipv4_plain(values));
        return;
    }

    if (kind == "string_mtf_rank") {
        if (parts.size() != 5) {
            throw std::runtime_error("string_mtf_rank expects rank and literal output files");
        }
        const auto encoded = semzip::encode_string_mtf_rank(values);
        write_bytes(parts[3], semzip::encode_varint_stream(encoded.ranks));
        write_bytes(parts[4], semzip::encode_string_stream(encoded.literals));
        return;
    }

    throw std::runtime_error("unsupported kind: " + kind);
}

}  // namespace

int main() {
    std::ios::sync_with_stdio(false);
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "QUIT") {
            std::cout << "BYE\n" << std::flush;
            return 0;
        }
        try {
            handle_write(split_tab(line));
            std::cout << "OK\n" << std::flush;
        } catch (const std::exception& exc) {
            std::cout << "ERR\t" << exc.what() << "\n" << std::flush;
        }
    }
    return 0;
}
