#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <fstream>

class Aggregator {
private:
    amqp_connection_state_t conn;
    std::string result_queue;
    int top_word_count;
    
    struct SectionResult {
        int word_count;
        std::map<std::string, int> word_frequencies;
        double sentiment_score;
        std::string sentiment_label;
        std::string processed_text;
    };
    
    std::map<std::string, SectionResult> results;
    int total_sections_expected {0};
    int total_sections_processed {0};
    int total_words;
    bool all_results_received;
    std::multimap<int, std::string, std::greater<int>> sentences_by_length;

public:
    Aggregator(const std::string& hostname, int port, int top_word_cnt, 
                      const std::string& queue = "result_queue") 
                      : top_word_count{top_word_cnt}, result_queue(queue), total_sections_processed(0), total_words(0) {
        
        conn = amqp_new_connection();
        amqp_socket_t* socket = amqp_tcp_socket_new(conn);
        
        if (!socket) {
            throw std::runtime_error("Cannot create TCP socket");
        }
        
        int status = amqp_socket_open(socket, hostname.c_str(), port);
        if (status) {
            throw std::runtime_error("Cannot connect to RabbitMQ");
        }
        
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, 1);
        amqp_get_rpc_reply(conn);
        
        amqp_queue_declare(conn, 1, amqp_cstring_bytes(result_queue.c_str()), 
                          0, 0, 0, 1, amqp_empty_table);
        amqp_get_rpc_reply(conn);
    }
    
    ~Aggregator() {
        amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
    }
    
    void parseResult(const std::string& message) {
        if (message.find("TOTAL_SECTIONS:") == 0) {
            total_sections_expected = std::stoi(message.substr(15));
            std::cout << "Expecting " << total_sections_expected << " sections total" << std::endl;
            return;
        }
        
        SectionResult result;
        std::string section_id;
        
        std::istringstream iss(message);
        std::string token;
        
        while (std::getline(iss, token, '|')) {
            if (token.find("SECTION_") == 0) {
                section_id = token;
            } else if (token.find("words:") == 0) {
                result.word_count = std::stoi(token.substr(6));
                total_words += result.word_count;
            } else if (token.find("top:") == 0) {
                parseTopWords(token.substr(4), result.word_frequencies);
            } else if (token.find("sentiment:") == 0) {
                parseSentiment(token.substr(10), result.sentiment_label, result.sentiment_score);
            } else if (token.find("sorted:") == 0) {
                std::string sentences_str = token.substr(7);
                std::istringstream sent_iss(sentences_str);
                std::cout << sent_iss.view() << std::endl;
                std::string sentence;
                while (std::getline(sent_iss, sentence, '~')) {
                    std::cout << sentence << std::endl;
                    if (!sentence.empty()) {
                        sentences_by_length.insert({sentence.length(), sentence});
                    }
                }
            } else if (token.find("processed_text:") == 0) {
                result.processed_text = token.substr(15);
            }
        }
        
        results[section_id] = result;
        total_sections_processed++;
        
        std::cout << "Aggregated result for " << section_id << " (" 
                << total_sections_processed << "/" << total_sections_expected << ")" << std::endl;
        
        if (total_sections_expected > 0 && total_sections_processed >= total_sections_expected) {
            all_results_received = true;
            std::cout << "All results received! Generating reports..." << std::endl;
        }
    }
    
    void parseTopWords(const std::string& top_words_str, std::map<std::string, int>& frequencies) {
        std::istringstream top_iss(top_words_str);
        std::string pair;
        
        while (std::getline(top_iss, pair, ';')) {
            size_t colon_pos = pair.find(':');
            if (colon_pos != std::string::npos) {
                std::string word = pair.substr(0, colon_pos);
                int count = std::stoi(pair.substr(colon_pos + 1));
                frequencies[word] += count;
            }
        }
    }
    
    void parseSentiment(const std::string& sentiment_str, std::string& label, double& score) {
        size_t colon_pos = sentiment_str.find(':');
        if (colon_pos != std::string::npos) {
            label = sentiment_str.substr(0, colon_pos);
            score = std::stod(sentiment_str.substr(colon_pos + 1));
        }
    }
    
    std::vector<std::pair<std::string, int>> getGlobalTopWords() {
        // aggregation
        std::map<std::string, int> global_freq;
        
        for (const auto& [section_id, result] : results) {
            for (const auto& [word, count] : result.word_frequencies) {
                global_freq[word] += count;
            }
        }
        
        std::vector<std::pair<std::string, int>> words(global_freq.begin(), global_freq.end());
        std::sort(words.begin(), words.end(),
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        if (top_word_count > words.size()) {
            top_word_count = words.size();
        }
        return std::vector<std::pair<std::string, int>>(words.begin(), words.begin() + top_word_count);
    }
    
    std::string getAggregatedSentiment() {
        // aggregation
        double total_score = 0.0;
        int positive_count = 0, negative_count = 0, neutral_count = 0;
        
        for (const auto& [section_id, result] : results) {
            total_score += result.sentiment_score;
            
            if (result.sentiment_label == "positive") positive_count++;
            else if (result.sentiment_label == "negative") negative_count++;
            else neutral_count++;
        }
        
        double avg_sentiment = total_score / results.size();
        
        std::stringstream ss;
        ss << "Average: " << avg_sentiment << " (Positive: " << positive_count 
           << ", Negative: " << negative_count << ", Neutral: " << neutral_count << ")";
        return ss.str();
    }
    
    void saveSortedText(const std::string& filename) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Cannot open file for writing: " << filename << std::endl;
            return;
        }
        
        int counter = 1;
        for (const auto& [length, sentence] : sentences_by_length) {
            file << sentence << "\n";
            counter++;
        }
    }

    void saveProcessedText(const std::string& filename) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Cannot open file for writing: " << filename << std::endl;
            return;
        }
        
        for (const auto& [sec_id, result] : results) {
            file << result.processed_text << "\n";
        }
    }
    
    void generateTextReport(const std::string& filename) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Cannot open file for writing: " << filename << std::endl;
            return;
        }
        
        auto top_words = getGlobalTopWords();

        file << "Sections processed: " << total_sections_processed << "\n";
        file << "Word count: " << total_words << "\n";
        file << "Sentiment result: " << getAggregatedSentiment() << "\n\n";
        
        file << "Top " << "10" << " words\n";
        for (size_t i = 0; i < top_words.size(); i++) {
            file << top_words[i].first << ": " << top_words[i].second << "\n";
        }
    }
    
    void collectResults() {
        amqp_basic_consume(conn, 1, amqp_cstring_bytes(result_queue.c_str()), 
                          amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_get_rpc_reply(conn);
        
        while (!all_results_received) {
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn);
            
            amqp_rpc_reply_t ret = amqp_consume_message(conn, &envelope, nullptr, 5000);
            
            if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
                std::string message((char*)envelope.message.body.bytes, 
                                   envelope.message.body.len);
                parseResult(message);
                amqp_destroy_envelope(&envelope);
            }
        }
        
        generateTextReport("report.txt");
        saveSortedText("sorted_text.txt");
        saveProcessedText("processed_text.txt");
    }
    
};

int main(int argc, char* argv[]) {
    try {
        int top_word_count = std::stoi(argv[1]);

        Aggregator aggregator("localhost", 5672, top_word_count);
        aggregator.collectResults();
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}