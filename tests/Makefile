
DATAWORKSPACES:=$(shell cd ../dataworkspaces; pwd)

help:
	@echo targets are: test clean mypy help install-rclone-deb

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration test_lineage_utils test_lineage test_snapshots test_jupyter_kit test_push_pull test_file_utils

MYPY_UTILS=workspace_utils.py lineage_utils.py git_utils.py
MYPY_BACKENDS=git.py
MYPY_RESOURCES=git_resource.py
MYPY_COMMANDS=init.py add.py snapshot.py
MYPY_APIS=lineage.py
MYPY_KITS=scikit_learn.py

test: clean mypy pyflakes
#	./testcli.sh --batch
#	python -m unittest $(UNIT_TESTS)

mypy:
	cd $(DATAWORKSPACES); mypy --ignore-missing-imports workspace.py
	cd $(DATAWORKSPACES)/utils; mypy --ignore-missing-imports $(MYPY_UTILS)
	cd $(DATAWORKSPACES)/backends; mypy --ignore-missing-imports $(MYPY_BACKENDS)
	cd $(DATAWORKSPACES)/resources; mypy --ignore-missing-imports $(MYPY_RESOURCES)
	cd $(DATAWORKSPACES)/commands; mypy --ignore-missing-imports $(MYPY_COMMANDS)
	#cd $(DATAWORKSPACES); mypy --ignore-missing-imports $(MYPY_APIS)
	#cd $(DATAWORKSPACES)/kits; mypy --ignore-missing-imports $(MYPY_KITS)

pyflakes:
	cd $(DATAWORKSPACES); pyflakes workspace.py
	cd $(DATAWORKSPACES)/utils; pyflakes $(MYPY_UTILS)
	cd $(DATAWORKSPACES)/backends; pyflakes $(MYPY_BACKENDS)
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
