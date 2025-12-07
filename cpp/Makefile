CXX = g++
CXXFLAGS = -std=c++20 -Wall -Wextra
LIBS = -lrabbitmq

all: producer worker aggregator

producer: producer.cpp
	$(CXX) $(CXXFLAGS) -I/usr/include/rabbitmq -o producer producer.cpp $(LIBS)

worker: worker.cpp
	$(CXX) $(CXXFLAGS) -o worker worker.cpp $(LIBS)

aggregator: aggregator.cpp
	$(CXX) $(CXXFLAGS) -o aggregator aggregator.cpp $(LIBS)

clean:
	rm -f producer worker aggregator *.log processed_text.txt report.txt sorted_text.txt

.PHONY: all clean