"""
A Python wrapper for rclone.
"""
# Original code:
# https://github.com/ddragosd/python-rclone
#
# Modifications:
# Rupak Majumdar 2019.01.01
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pylint: disable=W0102,W0703,C0103

import logging
import subprocess
import tempfile

from dataworkspaces.errors import ConfigurationError

class RCloneException(ConfigurationError):
    pass

class RClone:
    """
    Wrapper class for rclone.
    """

    def __init__(self, cfgfile=None, cfgstring=None):
        self.log = logging.getLogger("RClone")
        self._ensure_rclone_exists()
        self.cfgstring = ''
        self.cfgfile = None
        if cfgstring:
            self.cfgstring = cfgstring.replace("\\n", "\n")
        elif cfgfile:
            self.cfgfile = cfgstring.replace("\\n", "\n")
        else:
            # find the default config file used by the rclone installation
            ret = self._execute(['rclone', 'config', 'file'])
            print(ret)
            if ret['code'] == 0:
                # rclone config file output looks like:
                #
                # Configuration file is stored at:
                # filename
                # so we skip until the '\n'
                self.cfgfile = ret['out'].splitlines()[1].decode('utf_8')
            else:
                raise ConfigurationError("RClone requires either a configuration file or a configuration string")

        assert(self.cfgstring or self.cfgfile), 'Either a config string is given or a filename is given'

    def _ensure_rclone_exists(self):
        ret = self._execute(['rclone', 'version'])
        if ret['code'] == -20:
            raise ConfigurationError("rclone executable not found")

    def _execute(self, command_with_args):
        """
        Execute the given `command_with_args` using Popen
        Args:
            - command_with_args (list) : An array with the command to execute,
                                         and its arguments. Each argument is given
                                         as a new element in the list.
        """
        self.log.debug("Invoking : %s", " ".join(command_with_args))
        try:
            with subprocess.Popen(
                    command_with_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE) as proc:
                (out, err) = proc.communicate()

                #out = proc.stdout.read()
                #err = proc.stderr.read()

                self.log.debug(out)
                if err:
                    self.log.warning(err.decode("utf-8").replace("\\n", "\n"))

                return {
                    "code": proc.returncode,
                    "out": out,
                    "error": err
                }
        except FileNotFoundError as not_found_e:
            self.log.error("Executable not found. %s", not_found_e)
            return {
                "code": -20,
                "error": not_found_e
            }
        except Exception as generic_e:
            self.log.exception("Error running command. Reason: %s", generic_e)
            return {
                "code": -30,
                "error": generic_e
            }

    def run_cmd(self, command, extra_args=[]):
        """
        Execute rclone command
        Args:
            - command (string): the rclone command to execute.
            - extra_args (list): extra arguments to be passed to the rclone command
        """
        if self.cfgfile: # pass the file name to rclone
            command_with_args = ["rclone", command, "--config", self.cfgfile]
            command_with_args += extra_args
            command_result = self._execute(command_with_args)
            return command_result
     
        # config is given in a string 
        # save the configuration in a temporary file
        # and invoke rclone with the temporary file
        with tempfile.NamedTemporaryFile(mode='wt', delete=True) as cfg_file:
            # cfg_file is automatically cleaned up by python
            self.log.debug("rclone config: ~%s~", self.cfgstring)
            cfg_file.write(self.cfgstring)
            cfg_file.flush()

            command_with_args = ["rclone", command, "--config", cfg_file.name]
            command_with_args += extra_args
            command_result = self._execute(command_with_args)
            cfg_file.close()
            return command_result

    def copy(self, source, dest, flags=[]):
        """
        Executes: rclone copy source:path dest:path [flags]
        Args:
        - source (string): A string "source:path"
        - dest (string): A string "dest:path"
        - flags (list): Extra flags as per `rclone copy --help` flags.
        """
        return self.run_cmd(command="copy", extra_args=[source] + [dest] + flags)

    def sync(self, source, dest, flags=[]):
        """
        Executes: rclone sync source:path dest:path [flags]
        Args:
        - source (string): A string "source:path"
        - dest (string): A string "dest:path"
        - flags (list): Extra flags as per `rclone sync --help` flags.
        """
        return self.run_cmd(command="sync", extra_args=[source] + [dest] + flags)

    def listremotes(self, flags=[]):
        """
        Executes: rclone listremotes [flags]
        Args:
        - flags (list): Extra flags as per `rclone listremotes --help` flags.
        """
        ret = self.run_cmd(command="listremotes", extra_args=flags)
        if ret['code'] == 0:
            list_remotes = map((lambda s: s[0:-1].decode('utf_8')), ret['out'].split(b'\n'))
            print(list_remotes)
            return list(list_remotes)[0:-1]
        else:
            raise RCloneException('listremotes returns %d %s' % (ret['code'], ret['error']))

    def ls(self, dest, flags=[]):
        """
        Executes: rclone ls remote:path [flags]
        Args:
        - dest (string): A string "remote:path" representing the location to list.
        """
        return self.run_cmd(command="ls", extra_args=[dest] + flags)

    def lsjson(self, dest, flags=[]):
        """
        Executes: rclone lsjson remote:path [flags]
        Args:
        - dest (string): A string "remote:path" representing the location to list.
        """
        return self.run_cmd(command="lsjson", extra_args=[dest] + flags)

    def delete(self, dest, flags=[]):
        """
        Executes: rclone delete remote:path
        Args:
        - dest (string): A string "remote:path" representing the location to delete.
        """
        return self.run_cmd(command="delete", extra_args=[dest] + flags)

    def check(self, src, dest, flags=[]):
        """
        Executes: rclone check src dest 
        """
        ret = self.run_cmd(command="check", extra_args=[src, dest] + flags)
        if ret['code'] == 0:
            return (0, ret['out'] )
        else:
            raise RCloneException('rclone.check returns error %d (%s)' % (ret['code'], ret['error']))

def with_config(cfgstring):
    """
    Configure a new RClone instance.
    """
    inst = RClone(cfgstring=cfgstring)
    return inst

def test():
    rc = with_config("""[local]
        type = local
        nounc = true""")
    result = rc.listremotes()
    print("result = ", result)

    print("With default cfg file")
    rc = RClone()
    result = rc.listremotes()
    print("result = ", result)

if __name__ == "__main__":
    test()
