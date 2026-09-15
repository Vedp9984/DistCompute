CXX      ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra
BIN := bin
PROGS := prep_map prep_reduce sssp_map sssp_combine sssp_reduce final_map final_reduce

all: $(addprefix $(BIN)/,$(PROGS))

$(BIN):
	@mkdir -p $(BIN)

$(BIN)/%: src/%.cpp src/sssp_common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -rf $(BIN)

.PHONY: all clean
