CXX      ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra
BIN := bin
PROGS := wx_mapper wx_reducer wx_final

all: $(addprefix $(BIN)/,$(PROGS))

$(BIN):
	@mkdir -p $(BIN)

$(BIN)/%: src/%.cpp src/wx_partial.hpp src/weather_common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -rf $(BIN)

.PHONY: all clean
