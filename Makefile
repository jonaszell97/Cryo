.DEFAULT_GOAL := test-library-cryo-fast
SIMULATOR ?= iPhone 16 Pro
SIMULATOR_OS ?=
SIMULATOR_ID = $(shell sh Scripts/resolve-simulator.sh '$(SIMULATOR)' '$(SIMULATOR_OS)')
DESTINATION ?= platform=iOS Simulator,id=$(SIMULATOR_ID)
DERIVED_DATA_PATH ?= $(CURDIR)/DerivedData
TEST_OPTIONS ?= -skipMacroValidation CODE_SIGNING_ALLOWED=NO
RESULT_BUNDLE_OPTION = $(if $(RESULT_BUNDLE_PATH),-resultBundlePath '$(RESULT_BUNDLE_PATH)')

.PHONY: test-library-cryo test-library-cryo-fast test-all

test-library-cryo: ## Full package suite, including UIKit/CloudSyncable
	xcodebuild test -scheme Cryo -destination '$(DESTINATION)' \
	  -derivedDataPath '$(DERIVED_DATA_PATH)/Cryo' $(TEST_OPTIONS) $(RESULT_BUNDLE_OPTION) \
	  $(if $(SANITIZER),-enable$(SANITIZER)Sanitizer YES)

test-library-cryo-fast: ## macOS subset (excludes CloudSyncable)
	swift test $(if $(FILTER),--filter '$(FILTER)')

test-all: test-library-cryo
