# Zero dependency build: only a JDK 21+ is needed.
JAVAC ?= javac
JAVA  ?= java
MAIN_SRC := $(shell find src/main/java -name '*.java')
TEST_SRC := $(shell find src/test/java -name '*.java')

.PHONY: build test chaos sim mutations bench jar clean

build: out/main/.built

out/main/.built: $(MAIN_SRC)
	@mkdir -p out/main
	$(JAVAC) --release 21 -Xlint:all -Werror -d out/main $(MAIN_SRC)
	@touch $@

out/test/.built: out/main/.built $(TEST_SRC)
	@mkdir -p out/test
	$(JAVAC) --release 21 -Xlint:all -Werror -cp out/main -d out/test $(TEST_SRC)
	@touch $@

test: out/test/.built
	$(JAVA) -ea -cp out/main:out/test raftkv.TestRunner

# Longer randomized fault injection run: make chaos CHAOS_SECONDS=120
# Reproduce a failure exactly with: make chaos SEED=<seed printed by the failing run>
CHAOS_SECONDS ?= 60
chaos: out/test/.built
	$(JAVA) -ea -Dchaos.seconds=$(CHAOS_SECONDS) $(if $(SEED),-Dseed=$(SEED)) -cp out/main:out/test raftkv.TestRunner ChaosTest

# Deterministic simulation: make sim SEEDS=1000, replay one with make sim SEED=<seed>,
# or check a planted bug is caught with make sim SEEDS=300 MUTATION=read-without-quorum
SEEDS ?= 200
DURATION ?= 10000
sim: out/test/.built
	$(JAVA) -ea -Dsim.seeds=$(SEEDS) -Dsim.duration=$(DURATION) $(if $(SEED),-Dseed=$(SEED)) \
		$(if $(MUTATION),-Draftkv.mutation=$(MUTATION)) -cp out/main:out/test raftkv.sim.SimRunner

# Mutation testing: each planted protocol bug must be caught by the simulator.
mutations: out/test/.built
	@for m in read-without-quorum ack-before-sync no-election-restriction skip-dir-fsync; do \
		$(JAVA) -Draftkv.mutation=$$m -Dsim.seeds=300 -cp out/main:out/test raftkv.sim.SimRunner 2>/dev/null \
			| grep -E "mutation (caught|NOT)" | sed "s/^/$$m: /" || exit 1; done
	@$(JAVA) -Draftkv.mutation=commit-old-term -Dsim.seeds=3000 -cp out/main:out/test raftkv.sim.SimRunner \
		2>/dev/null | grep -E "mutation (caught|NOT)" | sed "s/^/commit-old-term: /"

bench: out/main/.built
	$(JAVA) -cp out/main raftkv.tools.Bench 5 64 10
	$(JAVA) -cp out/main raftkv.tools.Bench 5 1 10
	$(JAVA) -Draftkv.groupCommit=false -cp out/main raftkv.tools.Bench 5 64 10

jar: out/main/.built
	jar --create --file raftkv.jar --main-class raftkv.kv.KvServer -C out/main .

clean:
	rm -rf out raftkv.jar data
