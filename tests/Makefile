
DATAWORKSPACES:=$(shell cd ../dataworkspaces; pwd)

help:
	@echo targets are: test clean mypy help install-rclone-deb

UNIT_TESTS=test_git_utils test_move_results test_api test_git_fat_integration test_lineage_utils test_lineage test_snapshots test_jupyter_kit test_push_pull

MYPY_UTILS=workspace_utils.py lineage_utils.py
MYPY_APIS=lineage.py
MYPY_KITS=scikit_learn.py

test: clean mypy
	./testcli.sh --batch
	python -m unittest $(UNIT_TESTS)

mypy:
	cd $(DATAWORKSPACES)/utils; mypy --ignore-missing-imports $(MYPY_UTILS)
	cd $(DATAWORKSPACES); mypy --ignore-missing-imports $(MYPY_APIS)
	cd $(DATAWORKSPACES)/kits; mypy --ignore-missing-imports $(MYPY_KITS)

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

.PHONY: test help clean mypy install-rclone-deb
