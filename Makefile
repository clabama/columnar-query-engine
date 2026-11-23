OUT_MAIN := target/classes
OUT_TEST := target/test-classes
MAIN_SRC := $(shell find src/main/java -name '*.java')
TEST_SRC := $(shell find src/test/java -name '*.java')

.PHONY: all build test benchmark shell clean

all: build test

build:
	@mkdir -p $(OUT_MAIN)
	javac --release 21 -d $(OUT_MAIN) $(MAIN_SRC)

test: build
	@mkdir -p $(OUT_TEST)
	javac --release 21 -cp $(OUT_MAIN) -d $(OUT_TEST) $(TEST_SRC)
	java -cp $(OUT_MAIN):$(OUT_TEST) dev.wixforth.columnar.TestSuite

benchmark: build
	java -Xmx4g -cp $(OUT_MAIN) dev.wixforth.columnar.benchmark.BenchmarkMain --sf=1.0 --warmup=2 --runs=5

shell: build
	java -Xmx2g -cp $(OUT_MAIN) dev.wixforth.columnar.cli.SqlShellMain 0.1

clean:
	rm -rf target
