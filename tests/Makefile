
help:
	@echo targets are: test clean help

test:
	./testcli.sh --batch
	python test_move_results.py

clean:
	rm -rf ./test ./testresources
