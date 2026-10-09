#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <librdkafka/rdkafkacpp.h>

int main() {
    std::string brokers = std::getenv("KAFKA_BROKERS") ? std::getenv("KAFKA_BROKERS") : "localhost:9092";
    std::string errstr;
    
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return 1;
    }
    
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    std::cout << "Agent started. Reading from test.log..." << std::endl;
    std::ifstream log_file("test.log");
    std::string line;
    
    while (true) {
        if (std::getline(log_file, line)) {
            producer->produce("logs", RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                              (void *)line.c_str(), line.size(), NULL, 0, 0, NULL, NULL);
            producer->poll(0);
        } else {
            log_file.clear(); // Clear EOF flag
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    delete producer;
    return 0;
}
