#define PCRE2_CODE_UNIT_WIDTH 8

#include <pcre2.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

struct Spec {
    std::string tag;
    std::string pattern;
    std::string placeholder;
    std::string replacement;
    int store_group = 1;
    int context_group = 0;
    pcre2_code* code = nullptr;
};

std::string read_text_file(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open file: " + path);
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

void write_text_file(const std::string& path, const std::string& text) {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("cannot open output: " + path);
    }
    output.write(text.data(), static_cast<std::streamsize>(text.size()));
    if (!output) {
        throw std::runtime_error("failed writing output: " + path);
    }
}

std::vector<std::string> read_lines_keep_ends(const std::string& path) {
    const std::string text = read_text_file(path);
    std::vector<std::string> lines;
    std::size_t cursor = 0;
    while (cursor < text.size()) {
        const std::size_t next = text.find('\n', cursor);
        if (next == std::string::npos) {
            lines.push_back(text.substr(cursor));
            break;
        }
        lines.push_back(text.substr(cursor, next + 1 - cursor));
        cursor = next + 1;
    }
    if (text.empty()) {
        return lines;
    }
    return lines;
}

std::string read_record_line(std::istream& input, const std::string& field) {
    std::string line;
    if (!std::getline(input, line)) {
        throw std::runtime_error("truncated spec file while reading " + field);
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    return line;
}

std::vector<Spec> read_specs(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open specs: " + path);
    }
    std::vector<Spec> specs;
    while (true) {
        std::string marker;
        if (!std::getline(input, marker)) {
            break;
        }
        if (!marker.empty() && marker.back() == '\r') {
            marker.pop_back();
        }
        if (marker.empty()) {
            continue;
        }
        if (marker != "SPEC") {
            throw std::runtime_error("bad spec marker: " + marker);
        }
        Spec spec;
        spec.tag = read_record_line(input, "tag");
        spec.pattern = read_record_line(input, "pattern");
        spec.placeholder = read_record_line(input, "placeholder");
        spec.replacement = read_record_line(input, "replacement");
        spec.store_group = std::stoi(read_record_line(input, "store_group"));
        spec.context_group = std::stoi(read_record_line(input, "context_group"));
        const std::string end_marker = read_record_line(input, "END");
        if (end_marker != "END") {
            throw std::runtime_error("bad spec end marker: " + end_marker);
        }
        specs.push_back(std::move(spec));
    }
    return specs;
}

void compile_specs(std::vector<Spec>& specs) {
    for (auto& spec : specs) {
        int error_code = 0;
        PCRE2_SIZE error_offset = 0;
        spec.code = pcre2_compile(
            reinterpret_cast<PCRE2_SPTR>(spec.pattern.data()),
            spec.pattern.size(),
            0,
            &error_code,
            &error_offset,
            nullptr);
        if (spec.code == nullptr) {
            PCRE2_UCHAR message[256];
            pcre2_get_error_message(error_code, message, sizeof(message));
            std::ostringstream error;
            error << "PCRE2 compile failed for tag " << spec.tag << " at " << error_offset << ": " << message;
            throw std::runtime_error(error.str());
        }
    }
}

void free_specs(std::vector<Spec>& specs) {
    for (auto& spec : specs) {
        if (spec.code != nullptr) {
            pcre2_code_free(spec.code);
            spec.code = nullptr;
        }
    }
}

bool contains_any_placeholder(const std::string& text, const std::vector<std::string>& placeholders) {
    for (const auto& placeholder : placeholders) {
        if (!placeholder.empty() && text.find(placeholder) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::string group_text(const std::string& line, PCRE2_SIZE* ovector, int group) {
    const PCRE2_SIZE start = ovector[2 * group];
    const PCRE2_SIZE end = ovector[2 * group + 1];
    if (start == PCRE2_UNSET || end == PCRE2_UNSET || end < start) {
        return "";
    }
    return line.substr(static_cast<std::size_t>(start), static_cast<std::size_t>(end - start));
}

std::string json_escape(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (unsigned char ch : value) {
        if (ch == '\\' || ch == '"') {
            out.push_back('\\');
            out.push_back(static_cast<char>(ch));
        } else if (ch == '\b') {
            out += "\\b";
        } else if (ch == '\f') {
            out += "\\f";
        } else if (ch == '\n') {
            out += "\\n";
        } else if (ch == '\r') {
            out += "\\r";
        } else if (ch == '\t') {
            out += "\\t";
        } else if (ch < 0x20) {
            const char* hex = "0123456789abcdef";
            out += "\\u00";
            out.push_back(hex[(ch >> 4) & 0x0f]);
            out.push_back(hex[ch & 0x0f]);
        } else {
            out.push_back(static_cast<char>(ch));
        }
    }
    return out;
}

std::string json_string(const std::string& value) {
    return "\"" + json_escape(value) + "\"";
}

std::string stored_value_for_match(const Spec& spec, const std::string& raw_value, PCRE2_SIZE* ovector, const std::string& line) {
    if (spec.context_group > 0 && spec.context_group != spec.store_group) {
        const std::string context = group_text(line, ovector, spec.context_group);
        return "[" + json_string(raw_value) + "," + json_string(context) + "]";
    }
    return raw_value;
}

std::string format_replacement(const Spec& spec, PCRE2_SIZE* ovector, const std::string& line) {
    const std::string& replacement = spec.replacement;
    std::string out;
    std::size_t cursor = 0;
    while (cursor < replacement.size()) {
        const std::size_t open = replacement.find('{', cursor);
        if (open == std::string::npos) {
            out.append(replacement, cursor, std::string::npos);
            break;
        }
        out.append(replacement, cursor, open - cursor);
        const std::size_t close = replacement.find('}', open + 1);
        if (close == std::string::npos) {
            out.append(replacement, open, std::string::npos);
            break;
        }
        const std::string token = replacement.substr(open + 1, close - open - 1);
        if (token == "placeholder") {
            out += spec.placeholder;
        } else if (token.rfind("group", 0) == 0) {
            out += group_text(line, ovector, std::stoi(token.substr(5)));
        } else {
            out.append(replacement, open, close + 1 - open);
        }
        cursor = close + 1;
    }
    return out;
}

std::string apply_spec_to_line(
    const Spec& spec,
    const std::vector<std::string>& protected_placeholders,
    const std::string& input_line,
    std::vector<std::pair<std::string, std::string>>& values) {
    pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(spec.code, nullptr);
    if (match_data == nullptr) {
        throw std::runtime_error("cannot allocate match data for " + spec.tag);
    }

    std::string out;
    PCRE2_SIZE offset = 0;
    PCRE2_SIZE copied = 0;
    while (offset <= input_line.size()) {
        const int rc = pcre2_match(
            spec.code,
            reinterpret_cast<PCRE2_SPTR>(input_line.data()),
            input_line.size(),
            offset,
            0,
            match_data,
            nullptr);
        if (rc == PCRE2_ERROR_NOMATCH) {
            break;
        }
        if (rc < 0) {
            pcre2_match_data_free(match_data);
            throw std::runtime_error("PCRE2 match failed for " + spec.tag);
        }
        PCRE2_SIZE* ovector = pcre2_get_ovector_pointer(match_data);
        const PCRE2_SIZE start = ovector[0];
        const PCRE2_SIZE end = ovector[1];
        if (end < start || start < copied) {
            break;
        }
        const std::string whole = input_line.substr(static_cast<std::size_t>(start), static_cast<std::size_t>(end - start));
        if (spec.store_group >= rc || contains_any_placeholder(whole, protected_placeholders)) {
            offset = end > offset ? end : offset + 1;
            continue;
        }
        const std::string raw_value = group_text(input_line, ovector, spec.store_group);
        out.append(input_line, static_cast<std::size_t>(copied), static_cast<std::size_t>(start - copied));
        values.emplace_back(spec.tag, stored_value_for_match(spec, raw_value, ovector, input_line));
        out += format_replacement(spec, ovector, input_line);
        copied = end;
        if (end > offset) {
            offset = end;
        } else if (start < input_line.size()) {
            offset = start + 1;
        } else {
            break;
        }
    }
    out.append(input_line, static_cast<std::size_t>(copied), std::string::npos);
    pcre2_match_data_free(match_data);
    return out;
}

std::string apply_specs_to_line(
    const std::vector<Spec>& specs,
    const std::string& line,
    std::vector<std::pair<std::string, std::string>>& values) {
    std::string transformed = line;
    for (const auto& spec : specs) {
        std::vector<std::string> protected_placeholders;
        for (const auto& other : specs) {
            if (other.tag != spec.tag) {
                protected_placeholders.push_back(other.placeholder);
            }
        }
        transformed = apply_spec_to_line(spec, protected_placeholders, transformed, values);
    }
    return transformed;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: pcre2_replay_dump <spec_file> <input_log> <transformed_out> <values_jsonl_out>\n";
        return 2;
    }

    std::vector<Spec> specs;
    try {
        specs = read_specs(argv[1]);
        compile_specs(specs);
        const auto lines = read_lines_keep_ends(argv[2]);
        std::string transformed_text;
        std::vector<std::pair<std::string, std::string>> values;
        for (const auto& line : lines) {
            transformed_text += apply_specs_to_line(specs, line, values);
        }
        write_text_file(argv[3], transformed_text);
        std::ostringstream values_out;
        for (const auto& item : values) {
            values_out << "{\"tag\":" << json_string(item.first)
                       << ",\"value\":" << json_string(item.second) << "}\n";
        }
        write_text_file(argv[4], values_out.str());
        free_specs(specs);
        return 0;
    } catch (const std::exception& exc) {
        free_specs(specs);
        std::cerr << "ERROR\t" << exc.what() << "\n";
        return 1;
    }
}
