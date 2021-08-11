shell=/bin/bash
BASEDIR:=$(shell cd ..; pwd)
DATAWORKSPACES:=$(shell cd ../dataworkspaces; pwd)

help:
	@echo targets are: test clean mypy pyflakes check help install-rclone-deb format-with-black

UNIT_TESTS=test_git_utils test_file_utils test_move_results test_snapshots test_push_pull test_local_files_resource test_hashtree test_lineage_utils test_git_fat_integration test_git_lfs test_lineage test_jupyter_kit test_sklearn_kit test_api test_wrapper_utils test_tensorflow test_scratch_dir test_export test_import test_rclone test_alternative_branch test_s3_resource

MYPY_KITS=scikit_learn.py jupyter.py tensorflow.py wrapper_utils.py

test: clean mypy pyflakes
	./testcli.sh --batch
	python -m unittest $(UNIT_TESTS)

mypy:
	mypy --version
	cd $(BASEDIR); mypy  --config-file=mypy.ini dataworkspaces

pyflakes:
	cd $(DATAWORKSPACES); pyflakes workspace.py dws.py lineage.py api.py
	cd $(DATAWORKSPACES)/utils; pyflakes *.py
	cd $(DATAWORKSPACES)/backends; pyflakes *.py
	cd $(DATAWORKSPACES)/resources; pyflakes *.py
	cd $(DATAWORKSPACES)/resources/s3; pyflakes *.py
	cd $(DATAWORKSPACES)/commands; pyflakes *.py
	cd $(DATAWORKSPACES)/kits; pyflakes *.py

# shortcut for static checks
check: mypy pyflakes

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

# Warning - this will change the source code, use with care!
format-with-black:
	cd $(DATAWORKSPACES); black -l 100 -t py36 --exclude dataworkspaces/third_party .

clean:
	rm -rf ./test ./remotes ./clones rclone*.deb
	rm -rf ./utils_for_tests_data

.PHONY: test help clean mypy pyflakes check install-rclone-deb format-with-black
