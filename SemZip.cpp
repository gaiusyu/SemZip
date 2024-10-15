/*
LLMzip

*/


#define PCRE2_CODE_UNIT_WIDTH 8
#include <iostream>
#include <unordered_set>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <unordered_map>
#include <list>
#include <fstream>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <unordered_set>
#include <pcre2.h>
#include <stdexcept>
#include <future>
#include <filesystem>
#include <cstdlib>
#include <algorithm>
#include <queue>
#include <cmath>
#include <curl/curl.h>
#include <sstream>
#include <nlohmann/json.hpp>
#include <set>
#include "utfcpp/source/utf8.h"
std::queue<std::string> lines;
std::mutex mtx;



using json = nlohmann::json;

size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}


bool isValidUTF8(const std::string& str) {
    return utf8::is_valid(str.begin(), str.end());
}


std::string cleanInvalidUTF8(const std::string& str) {
    std::string output;
    utf8::replace_invalid(str.begin(), str.end(), std::back_inserter(output));
    return output;
}


bool areAllValidUTF8(const std::vector<std::string>& vec) {
    for (const auto& str : vec) {
        if (!isValidUTF8(str)) {
            return false;
        }
    }
    return true;
}


std::vector<std::string> cleanallInvalidUTF8(const std::vector<std::string>& vec) {
    std::vector<std::string> cleanVec;
    for (const auto& str : vec) {
        cleanVec.push_back(cleanInvalidUTF8(str));
    }
    return cleanVec;
}



bool areAllValidUTF8_map(const std::unordered_map<std::string, std::string>& map) {
    for (const auto& pair : map) {
        if (!isValidUTF8(pair.first) || !isValidUTF8(pair.second)) {
            return false;
        }
    }
    return true;
}

std::unordered_map<std::string, std::string> cleanAllInvalidUTF8_map(const std::unordered_map<std::string, std::string>& map) {
    std::unordered_map<std::string, std::string> cleanMap;
    for (const auto& pair : map) {
        std::string cleanKey = cleanInvalidUTF8(pair.first);
        std::string cleanValue = cleanInvalidUTF8(pair.second);
        cleanMap[cleanKey] = cleanValue;
    }
    return cleanMap;
}




int64_t zigzag_encode(int64_t num) {
    return (num << 1) ^ (num >> 63);
}

int64_t zigzag_decode(int64_t num) {
    return (num >> 1) ^ -(num & 1);
}

std::vector<unsigned char> elastic_encode(int64_t num) {
    std::vector<unsigned char> buffer;
    uint64_t cur = zigzag_encode(num);
    while (true) {
        if (cur < 0x80) {
            buffer.push_back(static_cast<unsigned char>(cur));
            break;
        } else {
            buffer.push_back(static_cast<unsigned char>((cur & 0x7F) | 0x80));
            cur >>= 7;
        }
    }
    return buffer;
}

int64_t elastic_decode(const std::vector<unsigned char>& num_bytes) {
    int64_t ret = 0;
    int offset = 0;
    for (auto cur : num_bytes) {
        ret |= (static_cast<int64_t>(cur & 0x7F) << offset);
        if ((cur & 0x80) == 0) {
            break;
        }
        offset += 7;
    }
    return zigzag_decode(ret);
}

std::vector<int64_t> elastic_decode_bytes(const std::vector<unsigned char>& binary_bytes) {
    std::vector<int64_t> num_list;
    std::vector<unsigned char> num_byte;
    for (auto byt : binary_bytes) {
        num_byte.push_back(byt);
        if (byt < 128) {
            int64_t decode_num = elastic_decode(num_byte);
            num_list.push_back(decode_num);
            num_byte.clear();
        }
    }
    return num_list;
}



struct RegexPattern {
    std::vector<std::string> patterns;
    std::vector<std::string> substitutions;
};

// LogProcessor class
class LogProcessor {
public:
    std::string logname;
    std::unordered_map<std::string, RegexPattern> regex_map;
    std::vector<pcre2_code *> compiled_patterns; // Used to store the compiled regular expression.
    pcre2_code *re_num;
    // Constructor.
    LogProcessor(const std::string &name) : logname(name) {
        // Predefined regular expressions and replacement symbols for different log names. You can modified these based on your systems.
        compile_num(&re_num, R"((?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9]))");
        regex_map["Android"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Apache"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2}) (\d+):(\d+):(\d+))"}, {"<I>", "<T>"} };
        regex_map["BGL"] = { {R"((\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+))",R"((\d+)\.(\d+)\.(\d+))"}, {"<E>", "<T>", "<F>"} };
        regex_map["Hadoop"] = { {R"((\d+)\-(\d+)\-(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<D>", "<T>"} };
        regex_map["HDFS"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<I>", "<T>"} };
        regex_map["HealthApp"] = { { R"((\d+):(\d+):(\d+):(\d+))"}, {"<T>"} };
        regex_map["HPC"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Linux"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>", "<T>"} };
        regex_map["Mac"] = { {R"((\d+)-(\d+)-(\d+)-(\d+))", R"((\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<D>", "<T>"} };
        regex_map["OpenSSH"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)",R"(sshd\[(\d+)\]:)"}, {"<I>", "<T>", "<S>"} };
        regex_map["OpenStack"] = { { R"(\.(\d+)-(\d+)-(\d+)_(\d+):(\d+):(\d+))",R"((\d+)-(\d+)-(\d+).(\d+):(\d+):(\d+)\.(\d+))",R"((\d+)\.(\d+)\.(\d+)\.(\d+),(\d+)\.(\d+)\.(\d+))"}, { "<D>", "<T>", "<I>"} };
        regex_map["Proxifier"] = { { R"((\d+)\.(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<T>"} };
        regex_map["Spark"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2})\/(\d{2})\/(\d{2}) (\d+):(\d+):(\d+))",R"((\d+)\.(\d{1}) MB)",R"((\d+)\.(\d{1}) KB)",R"((\d+)\.(\d{1}) GB)",R"((\d+)\.(\d{1}) B)"}, {"<I>", "<T>", "<M>", "<K>", "<G>", "<B>"} };
        regex_map["Thunderbird"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+):(\d+):(\d+))",R"((\d{4}})\.(\d+)\.(\d+))",R"(\[(\d+)\]:)"}, {"<I>","<T>","<A>","<B>"}};
        regex_map["Windows"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
        regex_map["Zookeeper"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+),(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
        // Compile the regular expression based on the log name.
        if (regex_map.find(logname) != regex_map.end()) {
            const auto &patterns = regex_map[logname].patterns;
            for (const auto &pattern : patterns) {
                pcre2_code *re = compile_pattern(pattern.c_str());
                compiled_patterns.push_back(re);
            }
        } else {
            throw std::runtime_error("Unknown logname");
        }
    }

    // Destructor.
    ~LogProcessor() {
        // Release the compiled regular expression.
        for (auto re : compiled_patterns) {
            pcre2_code_free(re);
        }
        pcre2_code_free(re_num);
    }


    std::tuple<std::vector<std::string>, std::vector<std::string>, std::unordered_map<std::string, std::vector<int64_t>>> removenumbers(const std::vector<std::string> &lst) {
        std::unordered_map<std::string, std::vector<int64_t>> patterns;
        std::vector<std::string> empirical_processed;
        std::vector<std::string> replaced;

        const std::string alpha = "abcdefghijklmnopqrstuvwxyz";
        const auto &substitutions = regex_map[logname].substitutions;

        for (const auto &item : lst) {
            std::string result = item;

            for (size_t i = 0; i < compiled_patterns.size(); ++i) {
                result = number_extract(result, compiled_patterns[i], substitutions[i], patterns);
            }
            empirical_processed.push_back(result);


            result = number_extract(result, re_num, alpha, patterns, true); 
            replaced.push_back(result);
        }

        return {replaced, empirical_processed, patterns};
    }
private:
    pcre2_code* compile_pattern(const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        pcre2_code *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
        return re;
    }
    void compile_num(pcre2_code **re, const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (*re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
    }



    std::string number_extract(const std::string &input, pcre2_code *re, const std::string &substitution, std::unordered_map<std::string, std::vector<int64_t>> &patterns, bool is_num = false) {
        std::string result;
        PCRE2_SIZE last_pos = 0;

        PCRE2_SPTR subject = (PCRE2_SPTR)input.c_str();
        size_t subject_length = strlen((char *)subject);

        pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, nullptr);

        int rc;
        while ((rc = pcre2_match(re, subject, subject_length, last_pos, 0, match_data, nullptr)) > 0) {
            PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);

            if (is_num) {
                std::string num = input.substr(ovector[0], ovector[1] - ovector[0]);
                size_t len = num.length();

                if (len == 1) {
                    std::string pattern_key = "<sn>";
                    patterns[pattern_key].push_back(std::stoll(num));
                    result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
                } else if (len < 15) {
                    std::string pattern_key = "[placeholder]";
                    patterns[pattern_key].push_back(std::stoll(num));
                    result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
                } else {
                    try {

                        int64_t number = std::stoll(num);
                        std::string pattern_key = "[bignum]";
                        patterns[pattern_key].push_back(number);
                        result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
                    } catch (const std::out_of_range &e) {

                        std::cerr << "Number out of range: " << num << std::endl;
                        result += input.substr(last_pos, ovector[1] - last_pos); 
                    }
                }
            } else {
                std::string match = input.substr(ovector[0], ovector[1] - ovector[0]);
                std::string num_str;
                for (char c : match) {
                    if (std::isdigit(c)) {
                        num_str += c;
                    }
                }
                size_t len = num_str.length();
                char len_char = 'A' + (len - 1);  
                std::string pattern_key = substitution ;
                if (true){
                        std::string pattern_key = "<"+substitution + "#"+ len_char+"#"+substitution+">";
                }

                patterns[pattern_key].push_back(std::stoll(num_str));
                result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
            }
            last_pos = ovector[1];
        }
        result += input.substr(last_pos);

        pcre2_match_data_free(match_data); 

        return result;
    }
};





class DenumLogProcessor {
private:
    std::string logname;

public:
    DenumLogProcessor(std::string name) : logname(name) {}

    std::vector<std::string> variable_extract(const std::vector<std::string>& logs, const std::string& chunkID) {
    std::vector<std::string> modified_lines;
    std::vector<std::string> variable_set;
    std::regex digit_pattern("\\d");
    std::regex regex_pattern;
    std::vector<std::string> delimiters;
    std::tie(regex_pattern, delimiters) = delimeter_mining(logs);
    std::string modified_line;
    std::vector<std::string> split;

    std::string last;
    std::unordered_map<std::string, std::vector<std::string>> pre_context;
    std::unordered_map<std::string, std::vector<std::string>> post_context;

    for (const auto& log : logs) {
        modified_line.clear();
        split = split_by_multiple_delimiters(regex_pattern, log, true);
        for (const auto& word : split) {
            if (std::regex_search(word, digit_pattern)) {
                modified_line += "<*>";
                variable_set.push_back(word);
            } else {
                modified_line += word;
            }
        }

        if (!last.empty()) {
            pre_context[modified_line].push_back(last);
            post_context[last].push_back(modified_line);
        }
        last = modified_line;
        modified_lines.push_back(modified_line);
    }

    std::vector<std::string> bound;
    for (const auto& [key, pre_values] : pre_context) {
        if (std::set<std::string>(pre_values.begin(), pre_values.end()).size() == 1) {
            if (std::set<std::string>(post_context[pre_values[0]].begin(), post_context[pre_values[0]].end()).size() == 1) {
                bound.push_back(key);
            }
        }
    }





    store_content_with_ids(variable_set, "variableset", chunkID, "lzma");
    return modified_lines;
}
    
    void ensure_directory_exists(const std::string& dir) {
        struct stat buffer;
        if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
            #ifdef _WIN32
            _mkdir(dir.c_str());  
            #else
            mkdir(dir.c_str(), 0777);  
            #endif
        }
    }


    void store_content_with_ids(const std::vector<std::string>& input, const std::string& output, const std::string& chunkID, const std::string& compressor) {
    std::unordered_map<std::string, int> content_to_id;
    std::unordered_map<int, std::string> id_to_content;
    int id_counter = 1;
    std::vector<int> id_list;
    std::string id_dir = "output/" + logname + "/" + chunkID + "/";
    std::string ids_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "ids.bin";
    std::string mapping_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "mapping.txt";
    ensure_directory_exists(id_dir);

    for (const auto& line : input) {
        if (line.empty()) continue;
        if (content_to_id.find(line) == content_to_id.end()) {
            content_to_id[line] = id_counter;
            id_to_content[id_counter] = line;
            id_counter++;
        }
        id_list.push_back(content_to_id[line]);
    }


    std::vector<std::pair<int, std::string>> sorted_content;
    for (const auto& pair : id_to_content) {
        sorted_content.push_back(pair);
    }

    // Sort by id_counter.
    std::sort(sorted_content.begin(), sorted_content.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    std::ofstream ids_file(ids_file_path, std::ios::binary);
    std::ofstream mapping_file(mapping_file_path);

    // Now write to mapping_file in the sorted order.
    for (const auto& pair : sorted_content) {
        mapping_file << pair.second << "\n";
    }

    for (int id : id_list) {
        auto encoded = elastic_encode(id);
        ids_file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
    }

    ids_file.close();
    mapping_file.close();
}

    std::string regex_escape(const std::string& pattern) {
            // List of special characters that need to be escaped.
            static const std::string special_chars = R"([-[\]{}()*+?.\\^$|])";

            // Construct the escaped pattern.
            std::string escaped_pattern;
            for (char c : pattern) {
                if (special_chars.find(c) != std::string::npos) {
                    escaped_pattern += '\\'; // Add escape characters.
                }
                escaped_pattern += c;
            }

            return escaped_pattern;
        }

    std::tuple<std::regex, std::vector<std::string>> delimeter_mining(const std::vector<std::string>& logs) {
        std::vector<std::string> temp = logs;
        std::random_shuffle(temp.begin(), temp.end());
        std::unordered_set<size_t> lengths;
        std::vector<std::string> sample;
        size_t iteration_count = 0;
        for (const auto& log : temp) {
            size_t log_len = log.size();
            if (lengths.find(log_len) == lengths.end()) {
                lengths.insert(log_len);
                sample.push_back(log);
            }
            iteration_count++;
            if (lengths.size() >= 10 || iteration_count >= 200) {
                break;
            }
        }
        std::vector<std::string> delimiters = find_special_chars_with_high_freq(sample);
        if (delimiters.empty()) {
            throw std::runtime_error("No delimiters found. Cannot create a valid regex pattern.");
        }

        std::string pattern_str = "(";
        for (const auto& delimiter : delimiters) {
            if (!delimiter.empty()) {
                pattern_str += regex_escape(delimiter) + "|";
            }
        }
        if (pattern_str.back() == '|') {
            pattern_str.pop_back(); // Remove the trailing "|".
        }
        pattern_str += ")";

        if (pattern_str == "()") {
            throw std::runtime_error("Invalid regex pattern: " + pattern_str);
        }
        return std::make_tuple(std::regex(pattern_str), delimiters);
    }

    std::vector<std::string> split_by_multiple_delimiters(const std::regex& pattern, const std::string& str, bool include_delimiters) {
        std::vector<std::string> result;
        auto words_begin = std::sregex_iterator(str.begin(), str.end(), pattern);
        auto words_end = std::sregex_iterator();

        size_t last_pos = 0;
        for (std::sregex_iterator iter = words_begin; iter != words_end; ++iter) {
            std::smatch match = *iter;
            size_t current_pos = match.position();
            if (current_pos > last_pos) {
                result.push_back(str.substr(last_pos, current_pos - last_pos));
            }
            if (include_delimiters) {
                result.push_back(match.str());
            }
            last_pos = current_pos + match.length();
        }

        if (last_pos < str.length()) {
            result.push_back(str.substr(last_pos));
        }

        return result;
    }


    std::vector<std::string> find_special_chars_with_high_freq(const std::vector<std::string>& str_list, size_t freq_threshold = 10) {
        std::vector<char> candidates = {',', ' ', '|', ';', '[', ']', '(', ')', '_', '/'};
        std::unordered_map<char, size_t> char_counter;
        for (const auto& s : str_list) {
            for (char c : s) {
                if (std::find(candidates.begin(), candidates.end(), c) != candidates.end()) {
                    char_counter[c]++;
                }
            }
        }

        std::vector<std::string> result;
        for (const auto& pair : char_counter) {
            if (pair.second > freq_threshold) {
                result.push_back(std::string(1, pair.first));
            }
        }

        // If the result is empty, add a space character.
        if (result.empty()) {
            result.push_back(" ");
        }

        return result;
    }
};



void ensure_directory_exists(const std::string& dir) {
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
        #ifdef _WIN32
        _mkdir(dir.c_str());  // Create directory on Windows system.
        #else
        mkdir(dir.c_str(), 0777);  // Create directory on Unix/Linux system.
        #endif
    }
}


std::string sanitize_filename(std::string filename) {
    std::replace(filename.begin(), filename.end(), '<', '_');  // Replace '<' with '_'.
    std::replace(filename.begin(), filename.end(), '>', '_');  // Replace '>' with '_'.
    return filename;
}

/*
delta_transform() : Calculate the difference between adjacent numbers.

input: number list

output: difference list
*/

std::vector<int64_t> delta_transform(const std::vector<int64_t>& num_list) {
    if (num_list.empty()) {
        return {}; 
    }

    std::vector<int64_t> new_list;
    new_list.reserve(num_list.size()); 

    int64_t initial = num_list[0];
    new_list.push_back(initial); 
    int64_t last = initial;

    for (size_t i = 1; i < num_list.size(); ++i) {
        int64_t delta = num_list[i] - last;
        new_list.push_back(delta);
        last = num_list[i];
    }

    return new_list;
}


void compressDirectory(const std::string& output_dir, int block_id) {
    std::string directoryPath = output_dir + "/" + std::to_string(block_id);
    std::string command = "tar -cJf " + output_dir + "/compressed" + std::to_string(block_id) + ".xz " + directoryPath;
    int result = std::system(command.c_str());

    if (result != 0) {
        std::cerr << "Command failed with return code: " << result << std::endl;
    } else {
        std::cout << "Block " << block_id << " directory successfully compressed into compressed" << block_id << ".xz" << std::endl;
    }
}

void printcountVector(const std::vector<std::pair<std::string, int>>& vec) {
    for (const auto& elem : vec) {
        std::cout << "{" << elem.first << ", " << elem.second << "}" << std::endl;
    }
}

std::tuple<std::vector<std::pair<std::string, int>>, std::vector<std::string>> generateProcessedLogs(const std::vector<std::string>& processed, const std::vector<std::string>& lst) {
    std::unordered_map<std::string, int> tempMap;
    std::vector<std::pair<std::string, int>> newMap; 
    std::vector<std::string> example;

    for (size_t i = 0; i < processed.size(); ++i) {
        const auto& p = processed[i];
        const auto& l = lst[i];
        
        if (tempMap.find(p) == tempMap.end()) {
            tempMap[p] = 0;
            newMap.emplace_back(p, 0);
            example.push_back(l); 
        } else {
            tempMap[p] += 1;
            for (auto& pair : newMap) {
                if (pair.first == p) {
                    pair.second = tempMap[p];
                    break;
                }
            }
        }
    }

    return std::make_tuple(newMap, example);
}

std::pair<std::vector<std::string>, std::vector<std::string>> selectTopFre(
    const std::vector<std::pair<std::string, int>>& processed_logs, 
    const std::vector<std::string>& log_index
) {
    std::vector<std::pair<std::string, std::string>> combined;
    std::cout << processed_logs.size() << std::endl;
    size_t index = 0;
    for (const auto& [key, value] : processed_logs) {
        if (value >= 25) {

            combined.emplace_back(key, log_index[index]);
        }
        ++index;
    }

    std::sort(combined.begin(), combined.end(), [&](const auto& a, const auto& b) {
 
        auto it_a = std::find_if(processed_logs.begin(), processed_logs.end(),
                                 [&](const auto& pair) { return pair.first == a.first; });
        auto it_b = std::find_if(processed_logs.begin(), processed_logs.end(),
                                 [&](const auto& pair) { return pair.first == b.first; });
        return it_a->second > it_b->second;
    });

    std::vector<std::string> top_processed_logs;
    std::vector<std::string> top_log_index;

    for (const auto& [key, idx] : combined) {
        top_processed_logs.push_back(key);
        top_log_index.push_back(idx);
    }

    return std::make_pair(top_processed_logs, top_log_index);
}


std::string gptCall(const std::string& prompt) {
    std::cerr << "Connecting to ChatGPT ... ..." << std::endl;
    const std::string api_url = "https://api.b3n.fun/v1/chat/completions";
    const std::string api_key = "YOURAPIKEY"; // Replace with your actual API key
    const int max_retries = 1;

    json request_body = {
        {"model", "gpt-4o"},
        {"messages", {{{"role", "user"}, {"content", prompt}}}},
        {"temperature", 0},
        {"seed", 0}
    };

    std::string request_data = request_body.dump();
    CURL* curl = curl_easy_init();
    CURLcode res;
    std::string readBuffer;

    if (!curl) {
        std::cerr << "Failed to initialize curl" << std::endl;
        return "";
    }

    for (int i = 0; i < max_retries; ++i) {
        readBuffer.clear();
        curl_easy_setopt(curl, CURLOPT_URL, api_url.c_str());

        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("Authorization: Bearer " + api_key).c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_data.c_str());

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);

        if (res == CURLE_OK) {
            try {
                // std::cerr << "Response JSON: " << readBuffer << std::endl;
                auto response_json = json::parse(readBuffer);
                if (!response_json["choices"].is_null() && !response_json["choices"].empty()) {
                    return response_json["choices"][0]["message"]["content"];
                }
            } catch (json::parse_error& e) {
                std::cerr << "JSON parse error: " << e.what() << std::endl;
                return "Failed to get a response";
            }
        } else {
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        curl_slist_free_all(headers);
    }
    

    curl_easy_cleanup(curl);
    std::cerr << "Failed to get a response" << std::endl;
    return "Failed to get a response";
}

std::string LLMzip_gptCalls(const std::string& secondPrompt) {
    std::string firstPrompt =  R"(
Your task is to replace "[placeholder]" in the log events with step1-tags based on context information. For each log event, you need to generate step1-tags that correspond to every "[placeholder]" in the event. "[placeholder]" with the same context should have the same step1-tag. The number of context characters you consider should depend on how many characters you think are needed to determine the meaning of the "[placeholder]".
Input Example:

Log event1: jk2_init() Can't find child [placeholder] in scoreboard
Example: jk2_init() Can't find child 4213 in scoreboard
Num of [placeholder] = 1
Log event2: jk2_init() Found child [placeholder] in scoreboard slot [placeholder]
Example: jk2_init() Found child 4214 in scoreboard slot 93
Num of [placeholder] = 2
Log event3: main:NIOServerCnxn@[placeholder]] - caught end of stream exception
Example: main:NIOServerCnxn@349] - caught end of stream exception
Num of [placeholder] = 1
Log event4: main:NIOServerCnxn@[placeholder]] - Closed socket connection for client [placeholder].[placeholder].[placeholder].[placeholder]:[placeholder]
Example: main:NIOServerCnxn@1001] - Closed socket connection for client 10.10.34.11:45303
Num of [placeholder] = 6
Log event5: Got assigned task [placeholder]
Example: Got assigned task 73912
Num of [placeholder] = 1
Log event6: Running task <sn>in stage [placeholder] (TID [placeholder])
Example: Running task 1 in stage 1834 (TID 73913)
Num of [placeholder] = 1

Analysis:

For log event1 and log event2, the context of "[placeholder]" indicates that both belong to childID, so they should be given the same step1-tag, i.e., <tag-a>. For the first [placeholder] in log event3 and log event4, it's not clear whether they belong to the same tag, you can assign tags based on context words. "NIOServerCnxn@[placeholder]] - caught" and "NIOServerCnxn@[placeholder]] - Closed" are different, so the first "[placeholder]" in log event3 and log event4 should be given different tags, i.e., <tag-b> and <tag-c> respectively. The remaining "[placeholder]" in log event4 is different parts of IP address. In addition, examples of every log events can help you decide step1-tags, "[placeholder]" that generates the same and similar numebrs are prone to be tagged the same. Finnally, each step1-tag will be followed by a number example. When you match numbers for step1-tags, note some numbers have been tagged as <sn>,<I> and etc., you should ignore these tags and only focus on "[placeholder]".

Output:
#begin#
E1: <tag-a>:4213
E2: <tag-a>:4214
E3: <tag-b>:349
E4: <tag-c>:1001,<tag-d>:10,<tag-e>:10,<tag-f>:34,<tag-g>:11,<tag-h>:45303
E5: <tag-ab>:73912
E6: <tag-ad>:73913
#end#

After understanding this task, I will provide a list of log events, and your response should follow the output example, the tags you generate cannot contain numbers.
)" + secondPrompt;
    // std::cout << "PROMPT:" << firstPrompt<< std::endl;
    // First API call
    std::string firstResponse = gptCall(firstPrompt);
    // std::cout << "Response:" << firstResponse <<std::endl;
    
    return firstResponse;
    // // Check if first response is successful or meet your criteria
    // if (!firstResponse.empty() && firstResponse != "Failed to get a response") {
    //     // Second API call
    //     std::string secondResponse = gptCall(secondPrompt);
    //     std::cout << "Second Response: " << secondResponse << std::endl;
    // } else {
    //     std::cerr << "First call failed, skipping second call." << std::endl;
    // }
}

int countOccurrences(const std::string& str, const std::string& substring) {
    if (substring.empty()) return 0;
    int count = 0;
    size_t pos = 0;
    while ((pos = str.find(substring, pos)) != std::string::npos) {
        ++count;
        pos += substring.length();
    }
    return count;
}

std::string prompt_generate(const std::vector<std::string>& top_processed_logs, const std::vector<std::string>& top_log_index, const std::unordered_map<std::string, std::string>& used_tags) {
    std::string result;
    std::string placeholder = "[placeholder]";


    result += "Ensure that the new tags you generate do not duplicate the following used tags:\n";
    for (const auto& pair : used_tags) {
        result += pair.first + ": " + pair.second + "\n";
    }
    result += "\n";

    for (size_t i = 0; i < top_processed_logs.size(); ++i) {
        result += "E" + std::to_string(i) + ": ";
        result += top_processed_logs[i] + "\n";
        result += "Example: ";
        result += top_log_index[i] + "\n";
        result += "Number of " + placeholder + " = " + std::to_string(countOccurrences(top_processed_logs[i], placeholder)) + "\n";
    }

    return result;
}

std::vector<std::string> find_placeholder_context(const std::string& s, const std::string& placeholder = "[placeholder]", int context_length = 6) {
    std::regex pattern(std::regex_replace(placeholder, std::regex(R"([\[\]])"), R"(\$&)")); // 转义方括号
    std::vector<std::string> contexts;
    std::sregex_iterator matches(s.begin(), s.end(), pattern);
    std::sregex_iterator end;

    for (std::sregex_iterator match = matches; match != end; ++match) {
        auto start = match->position();
        auto end = start + match->length();
        

        std::string before_context, after_context;

        int i = start - 1;
        while (before_context.size() < context_length && i >= 0) {
            if (!isdigit(s[i])) {
                before_context.push_back(s[i]);
            }
            --i;
        }
        std::reverse(before_context.begin(), before_context.end());


        i = end;
        while (after_context.size() < context_length && i < s.size()) {
            if (!isdigit(s[i])) {
                after_context.push_back(s[i]);
            }
            ++i;
        }

        if (before_context.size() < context_length) {
            before_context = std::string(context_length - before_context.size(), '-') + before_context;
        }


        if (after_context.size() < context_length) {
            after_context += std::string(context_length - after_context.size(), '-');
        }

        // std::cout << "Before context: " << before_context << ", After context: " << after_context << std::endl;

        contexts.push_back(before_context + " " + after_context);
    }

    return contexts;
}

void printVector(const std::vector<std::string>& vec) {
    for (const auto& str : vec) {
        std::cout << str << std::endl;
    }
}

void printContextToTag(const std::unordered_map<std::string, std::string>& context_to_tag) {
    for (const auto& pair : context_to_tag) {
        std::cout << "Context: " << pair.first << " -> Tag: " << pair.second << std::endl;
    }
}


std::unordered_map<std::string, std::string> rule_generate(const std::vector<std::string>& examples, const std::vector<std::vector<std::string>>& taglists) {
    std::unordered_map<std::string, std::string> context_to_tag;


    size_t min_size = std::min(examples.size(), taglists.size());

    for (size_t i = 0; i < min_size; ++i) {
        const std::string& example = examples[i];
        const std::vector<std::string>& tags = taglists[i];



        std::vector<std::string> contexts = find_placeholder_context(example);

        if (contexts.size() != tags.size()) {

            continue;
        }

        for (size_t j = 0; j < contexts.size(); ++j) {
            const std::string& context = contexts[j];
            const std::string& tag = tags[j];

            context_to_tag[context] = tag;
        }
    }
    // printContextToTag(context_to_tag);
    return context_to_tag;
}


std::string trim(const std::string& str) {
    const std::string whitespace = " \t\n\r";
    const auto begin = str.find_first_not_of(whitespace);
    if (begin == std::string::npos) return ""; // No content
    const auto end = str.find_last_not_of(whitespace);
    const auto range = end - begin + 1;
    return str.substr(begin, range);
}

std::vector<std::vector<std::string>> parse_string(const std::string& input) {
    std::vector<std::vector<std::string>> result;
    std::regex entry_regex(R"(E\d+:)");
    std::regex tag_regex(R"(<[^>]+>)");

    auto entry_begin = std::sregex_iterator(input.begin(), input.end(), entry_regex);
    auto entry_end = std::sregex_iterator();

    std::vector<std::string> entries;
    for (std::sregex_iterator i = entry_begin; i != entry_end; ++i) {
        entries.push_back(i->str());
    }

    std::string::size_type prev_pos = 0;
    for (const auto& entry : entries) {
        std::string::size_type pos = input.find(entry, prev_pos);
        if (pos == std::string::npos) {
            break;
        }
        
        std::string::size_type next_pos = input.find("E", pos + entry.length());
        std::string segment = input.substr(pos + entry.length(), next_pos - pos - entry.length());
        prev_pos = pos + entry.length();
        
        std::vector<std::string> tags;
        auto tag_begin = std::sregex_iterator(segment.begin(), segment.end(), tag_regex);
        auto tag_end = std::sregex_iterator();

        for (std::sregex_iterator j = tag_begin; j != tag_end; ++j) {
            tags.push_back(trim(j->str()));
        }

        result.push_back(tags);
    }

    return result;
}

void print_response_results(const std::vector<std::vector<std::string>>& response_results) {
    for (const auto& inner_vector : response_results) {
        std::cout << "[";
        for (const auto& item : inner_vector) {
            std::cout << item;
            if (&item != &inner_vector.back()) { 
                std::cout << ", ";
            }
        }
        std::cout << "]" << std::endl;
    }
}



std::pair<std::vector<std::string>, std::unordered_map<std::string, std::vector<int64_t>>> find_match(const std::vector<std::string>& input_list, 
    const std::unordered_map<std::string, std::string>& context_to_tag, 
    const std::unordered_map<std::string, std::vector<int64_t>>& num_dict) {

    std::string alpha = "abcdefghijklmnopqrstuvwxyz";
    const auto& nums = num_dict.at("[placeholder]");
    std::unordered_map<std::string, std::vector<int64_t>> tag_dict;
    std::vector<std::string> matches;

    size_t count = 0;

    for (const auto& item : input_list) {
        std::vector<std::string> context_list = find_placeholder_context(item);
        std::string item1 = item;
        for (const auto& cl : context_list) {
            std::string tag;
            if (context_to_tag.find(cl) != context_to_tag.end()) {
                tag = "<" + context_to_tag.at(cl) + "#" + std::string(1, alpha[std::to_string(nums[count]).length()]) + ">";
            } else {
                tag = "<mismatch#" + std::string(1, alpha[std::to_string(nums[count]).length()]) + ">";
            }
            tag_dict[tag].push_back(nums[count]);
            size_t pos = item1.find("[placeholder]");
            if (pos != std::string::npos) {
                item1.replace(pos, std::string("[placeholder]").length(), tag);
            }
            ++count;
        }
        matches.push_back(item1);
    }

    return {matches, tag_dict};
}

std::unordered_map<std::string, std::vector<int64_t>> merge_dicts(
    std::unordered_map<std::string, std::vector<int64_t>>& dict1, 
    const std::unordered_map<std::string, std::vector<int64_t>>& dict2) {
    
    for (const auto& [key, value] : dict2) {
        dict1[key].insert(dict1[key].end(), value.begin(), value.end());
    }
    return dict1;
}

void printNestedVector(const std::vector<std::vector<std::string>>& nestedVec) {
    for (const auto& vec : nestedVec) {
        std::cout << "[ ";
        for (const auto& str : vec) {
            std::cout << str << " ";
        }
        std::cout << "]" << std::endl;
    }
}


std::vector<int64_t> difference_list_generate(const std::vector<int64_t>& list) {
    std::vector<int64_t> difference_list;
    for (size_t i = 1; i < list.size(); ++i) {
        difference_list.push_back(list[i] - list[i - 1]);
    }
    return difference_list;
}

bool increment_test(const std::vector<int64_t>& list) {

    std::vector<int64_t> difference_list = difference_list_generate(list);


    std::unordered_set<int64_t> unique_differences(difference_list.begin(), difference_list.end());
    size_t a = unique_differences.size();

    std::unordered_set<int64_t> unique_elements(list.begin(), list.end());
    size_t b = unique_elements.size();


    return a <= 0.6* b;
}

bool all_elements_identical(const std::vector<int64_t>& vec) {
    return std::all_of(vec.begin(), vec.end(), [&](int64_t v){ return v == vec[0]; });
}



bool containsAll(const std::vector<std::string>& superSet, const std::vector<std::string>& subSet) {
    std::set<std::string> superSetSet(superSet.begin(), superSet.end());
    for (const auto& item : subSet) {
        if (superSetSet.find(item) == superSetSet.end()) {
            return false;
        }
    }
    return true;
}
bool containsSixtyPercent(const std::vector<std::string>& superSet, const std::vector<std::string>& subSet) {
    std::set<std::string> superSetSet(superSet.begin(), superSet.end());
    int count = 0;

    for (const auto& item : subSet) {
        if (superSetSet.find(item) == superSetSet.end()) {
            count++;
        }
    }

    int threshold = static_cast<int>(subSet.size() * 0.6 + 0.5);

    return count >= threshold;
}

std::mutex file_mutex;
std::mutex output_mutex;

std::vector<std::string> getFirst200(const std::vector<std::string>& data) {

    size_t count = std::min(data.size(), static_cast<size_t>(50));
    return std::vector<std::string>(data.begin(), data.begin() + count);
}


void processLogBlock(const std::vector<std::string>& block, int block_id, const std::string& output_dir, LogProcessor& log_processor, DenumLogProcessor& denum_processor, std::map<int, std::vector<std::string>>& final_outputs, const std::string& output_logs, const std::string& logname) {
    using json = nlohmann::json;


    std::string cache_file = "output/" + logname + "_cache.json";

    json cache_data;

    {


        std::lock_guard<std::mutex> lock(file_mutex);
        std::ifstream cache_ifs(cache_file);
        if (cache_ifs.is_open()) {
            try {
                cache_ifs >> cache_data;
            } catch (const std::exception& e) {
                std::cerr << "Failed to read cache file: " << e.what() << std::endl;
            }
            cache_ifs.close();
        } else {
            std::cerr << "Failed to open cache file for reading." << std::endl;
        }
    }


    std::vector<std::string> top_processed_logs;

    std::unordered_map<std::string, std::string> tag_rule;
    auto [final_output, empirical_processed, final_patterns] = log_processor.removenumbers(block);

    auto [newMap, example] = generateProcessedLogs(final_output, empirical_processed);
    
    std::tie(top_processed_logs, std::ignore) = selectTopFre(newMap, example);
    size_t totalCharacters = 0;
    for (const auto& str : top_processed_logs) {
        totalCharacters += str.length();
    }
    std::cout << "In this batch, before caching has " << totalCharacters << " characters." << std::endl;

    if (cache_data.contains("top_processed_logs") && cache_data.contains("tag_rule")) {
        std::cout << "Reading Cache ... ..." << std::endl;
        std::vector<std::string> cached_top_processed_logs = cache_data["top_processed_logs"].get<std::vector<std::string>>();
        tag_rule = cache_data["tag_rule"].get<std::unordered_map<std::string, std::string>>();
        auto input_top_processed_logs = getFirst200(top_processed_logs);
        if (containsAll(cached_top_processed_logs, input_top_processed_logs)) {
            std::cout << "Using cached tag_rule ..." << std::endl;
        } else {
            std::cout << "Cache doesn't contain all logs, calling GPT ..." << std::endl;
            std::vector<std::string> missing_logs;
            size_t totalCharacters_after = 0;

            auto input_missing_logs = missing_logs;

            std::set<std::string> cached_set(cached_top_processed_logs.begin(), cached_top_processed_logs.end());
            for (const auto& log : input_top_processed_logs) {
                    if (cached_set.find(log) == cached_set.end()) {
                        input_missing_logs.push_back(log);
                    }
                }
            size_t totalCharacters = 0;
            for (const auto& str : input_missing_logs) {
                totalCharacters += str.length();
            }
            std::cout << "In this batch, after caching has " << totalCharacters << " characters." << std::endl;
            std::string generated_prompt = prompt_generate(input_missing_logs, example, tag_rule);
            std::unordered_map<std::string, std::string> new_tag_rule;
            std::string LLM_response = LLMzip_gptCalls(generated_prompt);
            if (LLM_response == "Failed to get a response" || LLM_response.find("#begin") == std::string::npos) {
                if (LLM_response.find("#begin") == std::string::npos) {

                }

                new_tag_rule = std::unordered_map<std::string, std::string>(); 
            } else {
                std::vector<std::vector<std::string>> response_results = parse_string(LLM_response);
                new_tag_rule = rule_generate(input_missing_logs, response_results);
                for (const auto& pair : new_tag_rule) {
                    tag_rule[pair.first] = pair.second;
                }
            }


            cached_top_processed_logs.insert(cached_top_processed_logs.end(), input_missing_logs.begin(), input_missing_logs.end());
            if (!areAllValidUTF8(cached_top_processed_logs)) {
                std::cerr << "Invalid UTF-8 detected, cleaning data..." << std::endl;
                cached_top_processed_logs = cleanallInvalidUTF8(cached_top_processed_logs);
            }
            if (!areAllValidUTF8_map(tag_rule)) {
                std::cerr << "Invalid UTF-8 detected, cleaning data..." << std::endl;
                tag_rule = cleanAllInvalidUTF8_map(tag_rule);
            }


            cache_data["top_processed_logs"] = cached_top_processed_logs;

            cache_data["tag_rule"] = tag_rule;

            {
                std::lock_guard<std::mutex> lock(file_mutex);
                std::ofstream cache_ofs(cache_file);
                std::cout << "BBB" << std::endl;
                if (cache_ofs.is_open()) {
                    try {
                        cache_ofs << cache_data.dump(4);
                    } catch (const std::exception& e) {
                        std::cerr << "Failed to write cache file: " << e.what() << std::endl;
                    }
                    cache_ofs.close();
                } else {
                    std::cerr << "Failed to open cache file for writing." << std::endl;
                }
            }
        }
    } else {
        std::cout << "Cache not found, calling GPT ..." << std::endl;

        auto input_top_processed_logs = getFirst200(top_processed_logs);
        std::string generated_prompt = prompt_generate(input_top_processed_logs, example, tag_rule);
        std::string LLM_response = LLMzip_gptCalls(generated_prompt);
        if (LLM_response == "Failed to get a response" || LLM_response.find("#begin") == std::string::npos) {
            std::cout << "DDDD" << std::endl;
            cache_data["top_processed_logs"] = "";
            cache_data["tag_rule"] = "";
        } else {
            std::vector<std::vector<std::string>> response_results = parse_string(LLM_response);
            tag_rule = rule_generate(input_top_processed_logs, response_results);
        }
        if (!areAllValidUTF8_map(tag_rule)) {
                std::cerr << "Invalid UTF-8 detected, cleaning data..." << std::endl;
                tag_rule = cleanAllInvalidUTF8_map(tag_rule);
        }
        if (!areAllValidUTF8(input_top_processed_logs)) {
                std::cerr << "Invalid UTF-8 detected, cleaning data..." << std::endl;
                input_top_processed_logs = cleanallInvalidUTF8(input_top_processed_logs);
        }
        cache_data["top_processed_logs"] = input_top_processed_logs;
        cache_data["tag_rule"] = tag_rule;

        {
            std::lock_guard<std::mutex> lock(file_mutex);
            std::ofstream cache_ofs(cache_file);
            if (cache_ofs.is_open()) {
                try {
                    cache_ofs << cache_data.dump(4);
                } catch (const std::exception& e) {
                    std::cerr << "Failed to write cache file: " << e.what() << std::endl;
                }
                cache_ofs.close();
            } else {
                std::cerr << "Failed to open cache file for writing." << std::endl;
            }
        }
    }

    auto [logs_tags, nums_dict] = find_match(final_output, tag_rule, final_patterns);

    final_patterns.erase("[placeholder]");

    std::unordered_map<std::string, std::vector<int64_t>> final_dict = merge_dicts(nums_dict, final_patterns);

    std::string logname_dir = output_dir + "/" + std::to_string(block_id) + "/";
    ensure_directory_exists(logname_dir);
    std::vector<std::string> modified_logs = denum_processor.variable_extract(logs_tags, std::to_string(block_id));

    if (output_logs == "1") {
        denum_processor.store_content_with_ids(modified_logs, "all", std::to_string(block_id), "lzma");
    } else if (output_logs == "2") {
        std::lock_guard<std::mutex> lock(output_mutex);
        final_outputs[block_id] = modified_logs;
    } else if (output_logs == "3") {
        std::ofstream final_log_file(logname_dir + "logswithoutnums.log");
        if (!final_log_file.is_open()) {
            std::cerr << "Unable to open final log file for writing: " << logname_dir + "logswithoutnums.log" << std::endl;
            return;
        }
        for (const auto& log : modified_logs) {
            final_log_file << log << std::endl;
        }
        final_log_file.close();
    }

    for (const auto &pair : final_dict) {

        std::string sanitized_filename = sanitize_filename(pair.first);

        if (pair.first=="[bignum]") {
            std::string filename = logname_dir + sanitized_filename + ".txt";
            std::ofstream file(filename);
            for (size_t i = 0; i < pair.second.size(); ++i) {
                file << pair.second[i];
                if (i < pair.second.size() - 1) {
                    file << ",";
                }
            }
            continue;
        }
        std::string filename = logname_dir + sanitized_filename + ".bin";
        std::ofstream file(filename, std::ios::out | std::ios::binary);
        std::vector<int64_t> transformed;
        if (all_elements_identical(pair.second)) {
            transformed.push_back(pair.second.front());
            continue;
        }
        if (increment_test(pair.second)) {
            transformed = delta_transform(pair.second);
        } else {
            transformed = pair.second;
        }
        for (const auto& num : transformed) {
            auto encoded = elastic_encode(num);
            file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        }
        file.close();
    }

    compressDirectory(output_dir, block_id);
}


int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <logname> <block_size> <output_logs> <num_threads>" << std::endl;
        return 1;
    }

    const std::string logname = argv[1];
    size_t BLOCK_SIZE;
    const std::string output_logs = argv[3];
    int num_threads;

    try {
        BLOCK_SIZE = std::stoul(argv[2]);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid block size argument: " << argv[2] << std::endl;
        return 1;
    } catch (const std::out_of_range& e) {
        std::cerr << "Block size value out of range: " << argv[2] << std::endl;
        return 1;
    }

    try {
        num_threads = std::stoi(argv[4]);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid number of threads argument: " << argv[4] << std::endl;
        return 1;
    } catch (const std::out_of_range& e) {
        std::cerr << "Number of threads value out of range: " << argv[4] << std::endl;
        return 1;
    }

    std::cout << "Block Size: " << BLOCK_SIZE << std::endl;
    std::cout << "Number of Threads: " << num_threads << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    const std::string log_path = "Logs/" + logname + "/" + logname + ".log";
    
    std::vector<std::future<void>> futures;
    std::map<int, std::vector<std::string>> final_outputs;

    LogProcessor log_processor(logname);
    DenumLogProcessor denum_processor(logname);

    std::ifstream log_file(log_path);
    if (!log_file.is_open()) {
        std::cerr << "Unable to open log file: " << log_path << std::endl;
        return 1;
    }

    // Clean up and ensure the output directory exists.
    std::string command = "rm -rf output/" + logname + "/* ";
    int result = std::system(command.c_str());
    ensure_directory_exists("output/" + logname);

    std::vector<std::string> block;
    block.reserve(BLOCK_SIZE);
    std::string line;
    int block_index = 0;

    while (std::getline(log_file, line)) {
        block.push_back(line);
        if (block.size() == BLOCK_SIZE) {

            futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs), output_logs, logname));
            block.clear();
            ++block_index;

            if (futures.size() >= num_threads) {
                for (auto& future : futures) {
                    future.wait();
                }
                futures.clear();
            }
        }
    }

    // Process any remaining log lines (if any).
    if (!block.empty()) {
        futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs), output_logs, logname));
    }

    // Wait for all threads to complete.
    for (auto& future : futures) {
        future.wait();
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    std::uintmax_t fileSize = std::filesystem::file_size(log_path);
    double dataSizeInMB = static_cast<double>(fileSize) / (1024.0 * 1024.0);
    double speedInMBPerSecond = dataSizeInMB / duration.count() * 1000;
    double totalSize = 0;
    double totalBytes = 0;

    for (int i = 0; i <= block_index; ++i) {
        std::string compressed_path = "output/" + logname + "/compressed" + std::to_string(i) + ".xz";
        std::uintmax_t achieved_fileSize = std::filesystem::file_size(compressed_path);
        double dataSizeInMB = static_cast<double>(achieved_fileSize) / (1024.0 * 1024.0);
        totalSize += dataSizeInMB;
        totalBytes += achieved_fileSize;
    }

    double CR = dataSizeInMB / totalSize;

    // Output time taken CR&CS, achieved size, keeping three decimal places.
    std::cout << "Replacement completed in " << duration.count() << " milliseconds." << std::endl;
    std::cout << "Compression speed: " << std::fixed << std::setprecision(3) << speedInMBPerSecond << " MB/s" << std::endl;
    std::cout << "Achieved size: " << totalBytes << " Bytes" << std::endl;
    std::cout << "Compression ratio: " << std::fixed << std::setprecision(3) << CR << std::endl;

    if (output_logs == "2") {
        std::ofstream final_log_file("output/" + logname + ".log");
        for (const auto& [block_id, output] : final_outputs) {
            for (const auto& log : output) {
                final_log_file << log << std::endl;
            }
        }
        final_log_file.close();
    }

    return 0;
}