#include "semzip_codecs.hpp"

#include <algorithm>
#include <stdexcept>

namespace semzip {

std::vector<std::uint8_t> encode_varint(std::uint64_t value) {
    std::vector<std::uint8_t> out;
    while (true) {
        std::uint8_t chunk = static_cast<std::uint8_t>(value & 0x7fU);
        value >>= 7U;
        if (value != 0) {
            out.push_back(static_cast<std::uint8_t>(chunk | 0x80U));
        } else {
            out.push_back(chunk);
            return out;
        }
    }
}

std::pair<std::uint64_t, std::size_t> decode_varint(const std::vector<std::uint8_t>& data, std::size_t cursor) {
    std::uint64_t value = 0;
    unsigned shift = 0;
    while (true) {
        if (cursor >= data.size()) {
            throw std::runtime_error("truncated varint");
        }
        const std::uint8_t byte = data[cursor++];
        value |= (static_cast<std::uint64_t>(byte & 0x7fU) << shift);
        if ((byte & 0x80U) == 0) {
            return {value, cursor};
        }
        shift += 7;
        if (shift > 63) {
            throw std::runtime_error("varint too large");
        }
    }
}

std::uint64_t zigzag_encode(std::int64_t value) {
    return (static_cast<std::uint64_t>(value) << 1U) ^ static_cast<std::uint64_t>(value >> 63);
}

std::int64_t zigzag_decode(std::uint64_t value) {
    return static_cast<std::int64_t>((value >> 1U) ^ (~(value & 1U) + 1U));
}

std::vector<std::uint8_t> encode_delta_values(const std::vector<std::int64_t>& values) {
    std::vector<std::uint8_t> data;
    bool have_last = false;
    std::int64_t last = 0;
    for (std::int64_t value : values) {
        const std::int64_t delta = have_last ? (value - last) : value;
        auto encoded = encode_varint(zigzag_encode(delta));
        data.insert(data.end(), encoded.begin(), encoded.end());
        last = value;
        have_last = true;
    }
    return data;
}

std::vector<std::int64_t> decode_delta_values(const std::vector<std::uint8_t>& data) {
    std::vector<std::int64_t> values;
    std::size_t cursor = 0;
    bool have_last = false;
    std::int64_t last = 0;
    while (cursor < data.size()) {
        auto [encoded, next] = decode_varint(data, cursor);
        cursor = next;
        const std::int64_t delta = zigzag_decode(encoded);
        const std::int64_t value = have_last ? (last + delta) : delta;
        values.push_back(value);
        last = value;
        have_last = true;
    }
    return values;
}

std::vector<std::uint8_t> encode_ipv4_plain(const std::vector<std::string>& tokens) {
    std::vector<std::uint8_t> data;
    data.reserve(tokens.size() * 4);
    for (const auto& token : tokens) {
        std::size_t start = 0;
        int parts = 0;
        while (true) {
            const std::size_t dot = token.find('.', start);
            const std::string part = token.substr(start, dot == std::string::npos ? dot : dot - start);
            if (part.empty()) {
                throw std::runtime_error("bad IPv4 token");
            }
            if (part.size() > 1 && part[0] == '0') {
                throw std::runtime_error("IPv4 token is not reversible without widths");
            }
            int value = 0;
            for (char ch : part) {
                if (ch < '0' || ch > '9') {
                    throw std::runtime_error("bad IPv4 octet");
                }
                value = value * 10 + (ch - '0');
            }
            if (value < 0 || value > 255) {
                throw std::runtime_error("bad IPv4 octet");
            }
            data.push_back(static_cast<std::uint8_t>(value));
            ++parts;
            if (dot == std::string::npos) {
                break;
            }
            start = dot + 1;
        }
        if (parts != 4) {
            throw std::runtime_error("bad IPv4 token");
        }
    }
    return data;
}

std::vector<std::string> decode_ipv4_plain(const std::vector<std::uint8_t>& data) {
    if (data.size() % 4 != 0) {
        throw std::runtime_error("corrupt plain IPv4 stream");
    }
    std::vector<std::string> values;
    values.reserve(data.size() / 4);
    for (std::size_t i = 0; i < data.size(); i += 4) {
        values.push_back(
            std::to_string(data[i]) + "." + std::to_string(data[i + 1]) + "." +
            std::to_string(data[i + 2]) + "." + std::to_string(data[i + 3]));
    }
    return values;
}

std::vector<std::uint8_t> encode_string_stream(const std::vector<std::string>& values) {
    std::vector<std::uint8_t> data;
    for (const auto& value : values) {
        auto size = encode_varint(value.size());
        data.insert(data.end(), size.begin(), size.end());
        data.insert(data.end(), value.begin(), value.end());
    }
    return data;
}

std::vector<std::string> decode_string_stream(const std::vector<std::uint8_t>& data) {
    std::vector<std::string> values;
    std::size_t cursor = 0;
    while (cursor < data.size()) {
        auto [size, next] = decode_varint(data, cursor);
        cursor = next;
        if (cursor + size > data.size()) {
            throw std::runtime_error("corrupt string stream");
        }
        values.emplace_back(reinterpret_cast<const char*>(&data[cursor]), static_cast<std::size_t>(size));
        cursor += static_cast<std::size_t>(size);
    }
    return values;
}

MtfRankResult encode_string_mtf_rank(const std::vector<std::string>& values, std::size_t table_size) {
    std::vector<std::string> table;
    MtfRankResult result;
    for (const auto& value : values) {
        auto it = std::find(table.begin(), table.end(), value);
        if (it == table.end()) {
            result.ranks.push_back(0);
            result.literals.push_back(value);
            table.insert(table.begin(), value);
            if (table.size() > table_size) {
                table.resize(table_size);
            }
        } else {
            const std::size_t index = static_cast<std::size_t>(std::distance(table.begin(), it));
            result.ranks.push_back(index + 1);
            table.erase(it);
            table.insert(table.begin(), value);
        }
    }
    return result;
}

std::vector<std::string> decode_string_mtf_rank(
    const std::vector<std::uint64_t>& ranks,
    const std::vector<std::string>& literals,
    std::size_t table_size) {
    std::vector<std::string> table;
    std::vector<std::string> values;
    std::size_t literal_index = 0;
    for (std::uint64_t rank : ranks) {
        std::string value;
        if (rank == 0) {
            if (literal_index >= literals.size()) {
                throw std::runtime_error("string MTF literal stream exhausted");
            }
            value = literals[literal_index++];
            table.insert(table.begin(), value);
            if (table.size() > table_size) {
                table.resize(table_size);
            }
        } else {
            const std::size_t index = static_cast<std::size_t>(rank - 1);
            if (index >= table.size()) {
                throw std::runtime_error("bad string MTF rank");
            }
            value = table[index];
            table.erase(table.begin() + static_cast<std::ptrdiff_t>(index));
            table.insert(table.begin(), value);
        }
        values.push_back(value);
    }
    if (literal_index != literals.size()) {
        throw std::runtime_error("unused string MTF literals");
    }
    return values;
}

std::vector<std::uint8_t> encode_varint_stream(const std::vector<std::uint64_t>& values) {
    std::vector<std::uint8_t> data;
    for (std::uint64_t value : values) {
        auto encoded = encode_varint(value);
        data.insert(data.end(), encoded.begin(), encoded.end());
    }
    return data;
}

std::vector<std::uint64_t> decode_varint_stream(const std::vector<std::uint8_t>& data) {
    std::vector<std::uint64_t> values;
    std::size_t cursor = 0;
    while (cursor < data.size()) {
        auto [value, next] = decode_varint(data, cursor);
        cursor = next;
        values.push_back(value);
    }
    return values;
}

}  // namespace semzip
