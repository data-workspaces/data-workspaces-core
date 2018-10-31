
help:
	@echo targets are: test clean help

test: clean
	./testcli.sh --batch
	python test_move_results.py

clean:
	rm -rf ./test ./remotes

.PHONY: test help clean
