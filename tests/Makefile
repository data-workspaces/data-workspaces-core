
help:
	@echo targets are: test clean help

test: clean
	./testcli.sh --batch
	python test_move_results.py
	python test_git_utils.py

clean:
	rm -rf ./test ./remotes ./clones

.PHONY: test help clean
