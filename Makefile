PACKAGE=fslib
TESTS_DIR=tests
DOC_DIR=docs

# Use current python binary instead of system default.
COVERAGE = python $(shell which coverage)

.DEFAULT: help

define help
Makefile command help

Available targets are:

* Quality:
    pylint:	Run a pylint check on the code

* Testing:
    coverage:	Run the test suite and gather coverage reports
    test:	Run the test suite

* Misc:
    clean:      Cleanup all temporary files (*.pyc, ...)
    doc:        Generate the documentation
    help:       Display this help message
endef

help:
	@echo -n ""  # Don't display extra lines
	$(info $(help))

.PHONY: help


# Quality
# =======

pylint:
	pylint --rcfile=.pylintrc --report=no $(PACKAGE)/ || true

.PHONY: pylint


# Testing
# =======

coverage:
	$(COVERAGE) erase
	$(COVERAGE) run "--include=$(PACKAGE)/*.py,$(TESTS_DIR)/*.py" --branch setup.py test
	$(COVERAGE) report "--include=$(PACKAGE)/*.py,$(TESTS_DIR)/*.py"
	$(COVERAGE) html "--include=$(PACKAGE)/*.py,$(TESTS_DIR)/*.py"

test:
	python -W default setup.py test

.PHONY: coverage test


# Misc
# ====

clean:
	find . -type f -name '*.pyc' -delete
	find . -type f -path '*/__pycache__/*' -delete
	find . -type d -empty -delete


doc:
	$(MAKE) -C $(DOC_DIR) html


.PHONY: clean doc
