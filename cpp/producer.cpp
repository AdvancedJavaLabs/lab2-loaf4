#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>

class Producer {
private:
    amqp_connection_state_t conn;
    std::string task_queue;
    std::string result_queue;

public:
    Producer(const std::string& hostname, int port, 
                    const std::string& task_q = "task_queue",
                    const std::string& result_q = "result_queue") 
                    : task_queue(task_q), result_queue(result_q) {
        
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
    
    ~Producer() {
        amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
    }
    
    void sendToTaskQueue(const std::string& message) {
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;
        
        amqp_basic_publish(conn, 1, amqp_cstring_bytes(""),
                          amqp_cstring_bytes(task_queue.c_str()), 0, 0,
                          &props, amqp_cstring_bytes(message.c_str()));
    }
    
    void sendToResultQueue(const std::string& message) {
        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("text/plain");
        props.delivery_mode = 2;
        
        amqp_basic_publish(conn, 1, amqp_cstring_bytes(""),
                          amqp_cstring_bytes(result_queue.c_str()), 0, 0,
                          &props, amqp_cstring_bytes(message.c_str()));
    }
};

std::vector<std::string> readFileBySentences(const std::string& filename, int sentences_per_section) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    
    std::vector<std::string> sections;
    std::string current_section;
    int sentence_count = 0;
    std::string line;
    std::string accumulated_text;
    
    while (std::getline(file, line)) {
        if (!line.empty()) {
            if (!accumulated_text.empty() && accumulated_text.back() != ' ') {
                accumulated_text += " ";
            }
            accumulated_text += line;
        }
    }
    
    std::string current_sentence;
    for (char c : accumulated_text) {
        current_sentence += c;
        if (c == '.' || c == '!' || c == '?') {
            size_t start = current_sentence.find_first_not_of(" \n\r\t");
            size_t end = current_sentence.find_last_not_of(" \n\r\t");

            if (start != std::string::npos && end != std::string::npos) {
                std::string clean_sentence = current_sentence.substr(start, end - start + 1);
                
                if (!clean_sentence.empty()) {
                    current_section += clean_sentence + " ";
                    sentence_count++;
                    
                    if (sentence_count >= sentences_per_section) {
                        sections.push_back(current_section);
                        current_section.clear();
                        sentence_count = 0;
                    }
                }
            }
            current_sentence.clear();
        }
    }
    
    if (!current_section.empty()) {
        sections.push_back(current_section);
    }
    
    return sections;
}

int main(int argc, char* argv[]) {
    try {
        std::string filename = argv[1];
        int sentences_per_section = std::stoi(argv[2]);
        
        Producer producer("localhost", 5672);
        
        auto sections = readFileBySentences(filename, sentences_per_section);
        
        for (size_t i = 0; i < sections.size(); i++) {
            std::string message = "SECTION_" + std::to_string(i) + "|" + sections[i];
            producer.sendToTaskQueue(message);
            
            std::cout << "Sent " << i + 1 << "/" << sections.size() << " sections" << std::endl;
        }

        std::string count_message = "TOTAL_SECTIONS:" + std::to_string(sections.size());
        producer.sendToResultQueue(count_message);
        std::cout << "Sent total sections count: " << sections.size() << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}