
help:
	@echo targets are: test clean help

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration

test: clean
	./testcli.sh --batch
	python -m unittest $(UNIT_TESTS)

clean:
	rm -rf ./test ./remotes ./clones

.PHONY: test help clean
