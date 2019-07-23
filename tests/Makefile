shell=/bin/bash
MYPYPATH:=$(shell cd ..; pwd)
DATAWORKSPACES:=$(shell cd ../dataworkspaces; pwd)

help:
	@echo targets are: test clean mypy help install-rclone-deb

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration test_lineage_utils test_lineage test_snapshots test_jupyter_kit test_push_pull test_file_utils

MYPY_RESOURCES=git_resource.py
MYPY_COMMANDS=init.py add.py snapshot.py restore.py status.py push.py pull.py publish.py
MYPY_APIS=lineage.py
MYPY_KITS=scikit_learn.py

test: clean mypy pyflakes
#	./testcli.sh --batch
#	python -m unittest $(UNIT_TESTS)

mypy:
	cd $(DATAWORKSPACES); export MYPYPATH=$(MYPYPATH); mypy workspace.py
	cd $(DATAWORKSPACES); export MYPYPATH=$(MYPYPATH); mypy utils/
	cd $(DATAWORKSPACES); export MYPYPATH=$(MYPYPATH); mypy backends/
	cd $(DATAWORKSPACES)/resources; export MYPYPATH=$(MYPYPATH); mypy $(MYPY_RESOURCES)
	cd $(DATAWORKSPACES)/commands; export MYPYPATH=$(MYPYPATH); mypy $(MYPY_COMMANDS)
	#cd $(DATAWORKSPACES); mypy --ignore-missing-imports $(MYPY_APIS)
	#cd $(DATAWORKSPACES)/kits; mypy --ignore-missing-imports $(MYPY_KITS)

pyflakes:
	cd $(DATAWORKSPACES); pyflakes workspace.py dws.py
	cd $(DATAWORKSPACES)/utils; pyflakes *.py
	cd $(DATAWORKSPACES)/backends; pyflakes *.py
	cd $(DATAWORKSPACES)/resources; pyflakes $(MYPY_RESOURCES)
	cd $(DATAWORKSPACES)/commands; pyflakes $(MYPY_COMMANDS)
	#cd $(DATAWORKSPACES); pyflakes $(MYPY_APIS)

RCLONE_VERSION=v1.46
RCLONE_FILE=rclone-$(RCLONE_VERSION)-linux-amd64.deb
RCLONE_CONFIG=~/.config/rclone/rclone.conf
# install rclone for travis-ci
install-rclone-deb: $(RCLONE_FILE) $(RCLONE_CONFIG)
	sudo apt install -y ./$(RCLONE_FILE)

$(RCLONE_FILE):
	curl --output $(RCLONE_FILE) https://downloads.rclone.org/$(RCLONE_VERSION)/$(RCLONE_FILE)

$(RCLONE_CONFIG):
	mkdir -p ~/.config/rclone
	echo "[localfs]" >$(RCLONE_CONFIG)
	echo "type = local" >>$(RCLONE_CONFIG)
	echo "nounce =" >>$(RCLONE_CONFIG)

clean:
	rm -rf ./test ./remotes ./clones rclone*.deb

.PHONY: test help clean mypy pyflakes install-rclone-deb
