#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <algorithm>
#include <vector>
#include <regex>
#include <cctype>

class Worker {
private:
    amqp_connection_state_t conn;
    std::string task_queue;
    std::string result_queue;
    int top_word_count;
    
    std::vector<std::string> positive_words = {
        "good", "great", "excellent", "amazing", "wonderful", "fantastic",
        "happy", "joy", "love", "perfect", "beautiful", "nice", "best",
        "positive", "success", "win", "pleasure", "delight", "brilliant"
    };
    
    std::vector<std::string> negative_words = {
        "bad", "terrible", "awful", "horrible", "hate", "angry",
        "sad", "unhappy", "disappointing", "poor", "worst", "negative",
        "failure", "lose", "problem", "issue", "wrong", "broken"
    };

public:
    Worker(const std::string& hostname, int port, int top_word_cnt, 
                  const std::string& task_q = "task_queue",
                  const std::string& result_q = "result_queue") 
                  : top_word_count{top_word_cnt}, task_queue(task_q), result_queue(result_q) {
        
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
        
        amqp_queue_declare(conn, 1, amqp_cstring_bytes(task_queue.c_str()), 
                          0, 0, 0, 1, amqp_empty_table);
        amqp_get_rpc_reply(conn);
        
        amqp_queue_declare(conn, 1, amqp_cstring_bytes(result_queue.c_str()), 
                          0, 0, 0, 1, amqp_empty_table);
        amqp_get_rpc_reply(conn);
    }
    
    ~Worker() {
        amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
    }
    
    // word counter
    int countWords(const std::string& text) {
        std::istringstream iss(text);
        int count = 0;
        std::string word;
        
        while (iss >> word) {
            if (!word.empty() && !std::isalpha(word.back())) {
                word.pop_back();
            }
            if (!word.empty() && !std::isalpha(word.front())) {
                word = word.substr(1);
            }

            if (!word.empty()) {
                count++;
            }
        }
        return count;
    }
    
    // N top words finder
    std::string findTopWords(const std::string& text) {
        std::map<std::string, int> word_count;
        std::istringstream iss(text);
        std::string word;
        
        while (iss >> word) {
            word.erase(std::remove_if(word.begin(), word.end(), 
                                     [](unsigned char c) { return !std::isalpha(c); }), 
                      word.end());
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            
            if (!word.empty()) {
                word_count[word]++;
            }
        }
        
        std::vector<std::pair<std::string, int>> words(word_count.begin(), word_count.end());
        std::sort(words.begin(), words.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        std::string result;
        for (int i = 0; i < std::min(top_word_count, (int)words.size()); i++) {
            if (i > 0) result += ";";
            result += words[i].first + ":" + std::to_string(words[i].second);
        }
        
        return result;
    }
    
    // sentiment analyzer
    std::string analyzeSentiment(const std::string& text) {
        std::istringstream iss(text);
        std::string word;
        int positive_count = 0;
        int negative_count = 0;
        int total_words = 0;
        
        while (iss >> word) {
            word.erase(std::remove_if(word.begin(), word.end(), 
                                     [](unsigned char c) { return !std::isalpha(c); }), 
                      word.end());
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            
            if (!word.empty()) {
                total_words++;
                if (std::find(positive_words.begin(), positive_words.end(), word) != positive_words.end()) {
                    positive_count++;
                } else if (std::find(negative_words.begin(), negative_words.end(), word) != negative_words.end()) {
                    negative_count++;
                }
            }
        }
        
        if (total_words == 0) return "neutral:0";
        
        double sentiment_score = static_cast<double>(positive_count - negative_count) / total_words;
        
        if (sentiment_score > 0.1) {
            return "positive:" + std::to_string(sentiment_score);
        } else if (sentiment_score < -0.1) {
            return "negative:" + std::to_string(sentiment_score);
        } else {
            return "neutral:" + std::to_string(sentiment_score);
        }
    }
    
    // name replacer
    std::string replaceNames(const std::string& text, const std::string& replacement = "FFFFF") {
        std::regex name_regex("\\b[A-Z][a-z]+\\b");
        std::string result = std::regex_replace(text, name_regex, replacement);
        return result;
    }
    
    // legnth sentence sorter
    std::string sortSentencesByLength(const std::string& text) {
        std::vector<std::string> sentences;
        std::string current_sentence;
        
        for (char c : text) {
            current_sentence += c;
            if (c == '.' || c == '!' || c == '?') {
                size_t start = current_sentence.find_first_not_of(" \n\r\t");
                size_t end = current_sentence.find_last_not_of(" \n\r\t");
                if (start != std::string::npos && end != std::string::npos) {
                    std::string clean_sentence = current_sentence.substr(start, end - start + 1);
                    if (!clean_sentence.empty()) {
                        sentences.push_back(clean_sentence);
                    }
                }
                current_sentence.clear();
            }
        }
        
        if (!current_sentence.empty()) {
            size_t start = current_sentence.find_first_not_of(" \n\r\t");
            size_t end = current_sentence.find_last_not_of(" \n\r\t");
            if (start != std::string::npos && end != std::string::npos) {
                std::string clean_sentence = current_sentence.substr(start, end - start + 1);
                if (!clean_sentence.empty()) {
                    sentences.push_back(clean_sentence);
                }
            }
        }
        
        std::sort(sentences.begin(), sentences.end(),
                [](const std::string& a, const std::string& b) {
                    return a.length() > b.length();
                });
        
        std::string result;
        for (size_t i = 0; i < sentences.size(); i++) {
            if (i > 0) result += "~";
            result += sentences[i];
        }
        
        return result;
    }

    void sendResult(const std::string& result) {
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;
        
        amqp_basic_publish(conn, 1, amqp_cstring_bytes(""),
                          amqp_cstring_bytes(result_queue.c_str()), 0, 0,
                          &props, amqp_cstring_bytes(result.c_str()));
    }
    
    void processMessages() {
        amqp_basic_consume(conn, 1, amqp_cstring_bytes(task_queue.c_str()), 
                          amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_get_rpc_reply(conn);
        
        while (true) {
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn);
            
            amqp_rpc_reply_t ret = amqp_consume_message(conn, &envelope, nullptr, 0);
            
            if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
                std::string message((char*)envelope.message.body.bytes, 
                                   envelope.message.body.len);
                
                size_t pipe_pos = message.find('|');
                if (pipe_pos != std::string::npos) {
                    std::string section_id = message.substr(0, pipe_pos);
                    std::string text = message.substr(pipe_pos + 1);
                    
                    int word_count = countWords(text);
                    std::string top_words = findTopWords(text);
                    std::string sentiment = analyzeSentiment(text);
                    std::string name_replaced = replaceNames(text);
                    std::string sorted_sentences = sortSentencesByLength(text);
                    
                    std::string result = 
                        section_id + "|" +
                        "words:" + std::to_string(word_count) + "|" +
                        "top:" + top_words + "|" +
                        "sentiment:" + sentiment + "|" +
                        "names_replaced:" + std::to_string(name_replaced.length()) + "|" +
                        "processed_text:" + name_replaced + "|" +
                        "sorted:" + sorted_sentences;
                    
                    sendResult(result);
                    std::cout << "Processed and sent result: " << result << std::endl;
                }
                
                amqp_destroy_envelope(&envelope);
            }
        }
    }
};

int main(int argc, char* argv[]) {
    try {
        int top_word_count = std::stoi(argv[1]);

        Worker worker("localhost", 5672, top_word_count);
        worker.processMessages();
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}