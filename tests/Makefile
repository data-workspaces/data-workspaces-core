
DATAWORKSPACES:=$(shell cd ../dataworkspaces; pwd)

help:
	@echo targets are: test clean mypy help

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration test_lineage_utils test_lineage

MYPY_UTILS=workspace_utils.py lineage_utils.py
MYPY_APIS=lineage.py

test: clean mypy
	./testcli.sh --batch
	python -m unittest $(UNIT_TESTS)

mypy:
	cd $(DATAWORKSPACES)/utils; mypy --ignore-missing-imports $(MYPY_UTILS); \
    cd $(DATAWORKSPACES); mypy --ignore-missing-imports $(MYPY_APIS)

clean:
	rm -rf ./test ./remotes ./clones

.PHONY: test help clean mypy
