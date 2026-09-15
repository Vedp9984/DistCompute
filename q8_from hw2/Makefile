# Q8 -- Large-scale weather / environmental data analytics
CXX      ?= g++
MPICXX   ?= mpicxx
# OMPI_SKIP_MPICXX drops OpenMPI's deprecated C++ bindings header, which is the
# only thing that warns under -Wextra.  The C interface is what we use.
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -DOMPI_SKIP_MPICXX

BIN := bin
SRC := src

all: $(BIN)/weather_mpi $(BIN)/weather_seq $(BIN)/gen_weather

$(BIN):
	@mkdir -p $(BIN)

$(BIN)/weather_mpi: $(SRC)/weather_mpi.cpp $(SRC)/weather_common.hpp | $(BIN)
	$(MPICXX) $(CXXFLAGS) -o $@ $<

$(BIN)/weather_seq: $(SRC)/weather_seq.cpp $(SRC)/weather_common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $<

$(BIN)/gen_weather: $(SRC)/gen_weather.cpp | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $<

data: $(BIN)/gen_weather
	@bash scripts/gen_data.sh

check: all
	@bash scripts/verify.sh

bench: all data
	@bash scripts/benchmark.sh

clean:
	rm -rf $(BIN)

distclean: clean
	rm -rf data

.PHONY: all data check bench clean distclean
