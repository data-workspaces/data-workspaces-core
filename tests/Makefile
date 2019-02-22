
help:
	@echo targets are: test clean mypy help

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration test_lineage_utils

MYPY_UTILS=lineage_utils.py

test: clean
	./testcli.sh --batch
	python -m unittest $(UNIT_TESTS)

mypy:
	cd ../dataworkspaces/utils; mypy --ignore-missing-imports $(MYPY_UTILS)

clean:
	rm -rf ./test ./remotes ./clones

.PHONY: test help clean mypy
