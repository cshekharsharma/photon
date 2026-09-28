# Makefile for Photon

# This Makefile defines several commands to manage and build the project omega.
# The commands include linting, testing, cleaning, and running the Photon,
# as well as configuring the project and generating test coverage reports.

# Colors
RED         := \033[0;31m
GREEN       := \033[0;32m
YELLOW      := \033[1;33m
BLUE        := \033[0;34m
NC          := \033[0m

# Go Commands
MAKE        := make # Command to run Makefile
GOCMD       := go   # Command to run Go
GOBUILD     := $(GOCMD) build   # Command to build Go binaries
GOCLEAN     := $(GOCMD) clean   # Command to clean Go binaries and caches
GOFMT       := gofmt            # Command to format Go source code
GOINSTALL   := $(GOCMD) install # Command to install go packages
GO_BIN      := $(shell $(GOCMD) env GOPATH)/bin
GOIMPORT    := $(GO_BIN)/goimports        # Command to adjust/fix Go imports
GOLINT      := $(GO_BIN)/golangci-lint    # Command to run golangci-lint for Go code
STATICCHECK := $(GO_BIN)/staticcheck      # Command to run static analysis on Go code
GOVULNCHECK := $(GO_BIN)/govulncheck      # Command to run Go vulnerability checks

GOIMPORTS_VERSION      := v0.50.0
STATICCHECK_VERSION    := 2026.2.1
GOLANGCI_LINT_VERSION  := v2.13.2
GOVULNCHECK_VERSION    := v1.8.0

# Target: help
# Description: List all make targets with descriptions
.PHONY: help
help:
	@printf "\n${YELLOW}Makefile Targets:${NC}\n\n"
	@printf "  ${GREEN}clean${NC}          - Clean the previous builds\n"
	@printf "  ${GREEN}configure${NC}      - Configure the project and install local hooks\n"
	@printf "  ${GREEN}tools${NC}          - Install pinned local development tools\n"
	@printf "  ${GREEN}install-hooks${NC}  - Install local git hooks when .git exists\n"
	@printf "  ${GREEN}install${NC}        - Alias for configure command\n"
	@printf "  ${GREEN}lint${NC}           - Run linting & static checks for go code\n"
	@printf "  ${GREEN}test${NC}           - Run unit tests\n"
	@printf "  ${GREEN}testcoverage${NC}   - Run unit tests with coverage report\n"
	@printf "  ${GREEN}all${NC}            - Run all important steps from the list\n\n"

# Target: tools
# Description: Install pinned developer and release-check tooling.
.PHONY: tools
tools:
	$(GOINSTALL) golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)
	$(GOINSTALL) honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
	$(GOINSTALL) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GOINSTALL) golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

# Target: configure
# Description: Configure the project by tidying and verifying the modules,
# formatting the code, running static analysis, and installing local hooks.
.PHONY: configure
configure:
	@printf "\n${YELLOW}CONFIGURING THE PACKAGE...${NC}\n\n"
	export GOPRIVATE=github.com
	$(GOCMD) mod tidy
	$(GOCMD) mod verify
	$(GOIMPORT) -w .
	$(GOFMT) -s -w .
	$(STATICCHECK) ./...
	@$(MAKE) install-hooks
	@printf "\n✅ Project successfully configured.\n"

# Target: install-hooks
# Description: Install local git hooks for developer workstations.
.PHONY: install-hooks
install-hooks:
	@if [ ! -d .git ]; then \
		printf "\n${YELLOW}Skipping hook installation: .git directory not found.${NC}\n"; \
		exit 0; \
	fi
	cp ./tools/pre-commit.sh ./.git/hooks/pre-commit
	chmod +x ./.git/hooks/pre-commit
	cp ./tools/pre-push.sh ./.git/hooks/pre-push
	chmod +x ./.git/hooks/pre-push
	@printf "\n✅ Git hooks installed.\n"

# Target: install
# Description: Install the dependencies and configure the project (alias for configure).
.PHONY: install
install: configure

# Target: lint
# Description: Run static code analysis using golangci-lint
.PHONY: lint
lint:
	@printf "\n${YELLOW}LINTING THE CODEBASE...${NC}\n\n"
	$(GOIMPORT) -w .
	$(GOFMT) -s -w .
	$(STATICCHECK) ./...
	$(GOLINT) run ./...
	@printf "\n✅ Project successfully linted and analysed.\n"

# Target: test
# Description: Run unit tests for the project.
.PHONY: test
test:
	@printf "\n${YELLOW}RUNNING UNIT TESTS...${NC}\n\n"
	rm -rf ./coverage.txt
	$(GOCMD) run tools/gotest_exec.go --skip-mocks
	@printf "\n✅ Testcase execution completed.\n"

# Target: testcoverage
# Description: Run unit tests and generate a coverage report for the project.
.PHONY: testcoverage
testcoverage:
	@printf "\n${YELLOW}RUNNING UNIT TESTS WITH COVERAGE REPORT...${NC}\n\n"
	rm -rf ./coverage.txt
	$(GOCMD) run tools/gotest_exec.go --skip-mocks
	$(GOCMD) run tools/gotest_coverage.go
	@printf "\n✅ Testcase execution completed with coverage report.\n\n"

# Target: clean
# Description: Clean the previous builds and remove the binary.
.PHONY: clean
clean:
	@printf "\n${YELLOW}CLEANING THE ENVIRONMENT...${NC}\n\n"
	$(GOCLEAN)
	@printf "\n✅ Project cleaned.\n"


# Target: all
# Description: configure and test the package
.PHONY: all
all:
	@$(MAKE) clean
	@$(MAKE) configure
	@$(MAKE) testcoverage
	@printf "\n✅ All steps completed successfully.\n\n"
