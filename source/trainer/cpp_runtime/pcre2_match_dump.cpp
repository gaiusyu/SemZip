#define PCRE2_CODE_UNIT_WIDTH 8

#include <pcre2.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::string read_text_file(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open file: " + path);
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    std::string text = buffer.str();
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
        text.pop_back();
    }
    return text;
}

std::vector<std::string> read_lines(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open input: " + path);
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
}

void print_span(PCRE2_SIZE start, PCRE2_SIZE end) {
    if (start == PCRE2_UNSET || end == PCRE2_UNSET) {
        std::cout << "-1:-1";
    } else {
        std::cout << static_cast<std::uint64_t>(start) << ":" << static_cast<std::uint64_t>(end);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: pcre2_match_dump <pattern_file> <input_lines_file>\n";
        return 2;
    }

    try {
        const std::string pattern = read_text_file(argv[1]);
        const auto lines = read_lines(argv[2]);

        int error_code = 0;
        PCRE2_SIZE error_offset = 0;
        pcre2_code* re = pcre2_compile(
            reinterpret_cast<PCRE2_SPTR>(pattern.data()),
            pattern.size(),
            0,
            &error_code,
            &error_offset,
            nullptr);
        if (re == nullptr) {
            PCRE2_UCHAR message[256];
            pcre2_get_error_message(error_code, message, sizeof(message));
            std::cerr << "PCRE2_COMPILE_ERROR\t" << error_offset << "\t" << message << "\n";
            return 3;
        }

        pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(re, nullptr);
        if (match_data == nullptr) {
            pcre2_code_free(re);
            throw std::runtime_error("cannot allocate match data");
        }

        for (std::size_t line_index = 0; line_index < lines.size(); ++line_index) {
            const std::string& line = lines[line_index];
            PCRE2_SIZE offset = 0;
            while (offset <= line.size()) {
                const int rc = pcre2_match(
                    re,
                    reinterpret_cast<PCRE2_SPTR>(line.data()),
                    line.size(),
                    offset,
                    0,
                    match_data,
                    nullptr);
                if (rc == PCRE2_ERROR_NOMATCH) {
                    break;
                }
                if (rc < 0) {
                    std::cerr << "PCRE2_MATCH_ERROR\t" << line_index << "\t" << rc << "\n";
                    pcre2_match_data_free(match_data);
                    pcre2_code_free(re);
                    return 4;
                }

                PCRE2_SIZE* ovector = pcre2_get_ovector_pointer(match_data);
                std::cout << line_index << "\t";
                print_span(ovector[0], ovector[1]);
                std::cout << "\t" << rc;
                for (int group = 1; group < rc; ++group) {
                    std::cout << "\t";
                    print_span(ovector[2 * group], ovector[2 * group + 1]);
                }
                std::cout << "\n";

                const PCRE2_SIZE match_start = ovector[0];
                const PCRE2_SIZE match_end = ovector[1];
                if (match_end > offset) {
                    offset = match_end;
                } else if (match_start < line.size()) {
                    offset = match_start + 1;
                } else {
                    break;
                }
            }
        }

        pcre2_match_data_free(match_data);
        pcre2_code_free(re);
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << "ERROR\t" << exc.what() << "\n";
        return 1;
    }
}
