
help:
	@echo targets are: test clean help

test: clean
	./testcli.sh --batch
	python test_move_results.py
	python test_git_utils.py
	python test_api.py
	python test_git_fat_integration.py

clean:
	rm -rf ./test ./remotes ./clones

.PHONY: test help clean
