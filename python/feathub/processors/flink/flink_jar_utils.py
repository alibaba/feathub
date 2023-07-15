#  Copyright 2022 The FeatHub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import glob
import os

from pyflink.table import StreamTableEnvironment

from feathub.common.exceptions import FeathubException


def find_jar_lib() -> str:
    """
    Find the path to the directory of jars that the Flink processor requires.

    If we are in the development environment, we use the jars in the development
    directory so that developer doesn't have to install the FeatHub every time when
    change is made to the Java code. If we are not in the development, we use the jars
    that are packaged in the FeatHub wheels.
    """
    current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    feathub_root = os.path.abspath(current_dir + "/../../../../")
    build_target = glob.glob(
        feathub_root + "/java/feathub-dist/target/feathub-dist-*-bin/feathub-dist-*"
    )
    if len(build_target) > 0:
        # We are in the FeatHub development environment.
        return os.path.join(build_target[0], "lib")

    for feathub_module_path in __import__("feathub").__path__:
        lib_dir_path = os.path.join(feathub_module_path, "processors", "flink", "lib")
        if not os.path.exists(lib_dir_path):
            raise FeathubException(f"Cannot find the lib for FeatHub at {lib_dir_path}")
        return lib_dir_path

    raise FeathubException(
        "Could not the path to the FeatHub Flink jar directory in current environment."
    )


def add_jar_to_t_env(t_env: StreamTableEnvironment, *jar_paths: str) -> None:
    """
    Add each jar path to the given StreamTableEnvironment if it is not already added.
    """
    old_jars = t_env.get_config().get("pipeline.jars", "")
    old_jars = [] if old_jars == "" else old_jars.split(";")
    jars = [f"file://{jar_path}" for jar_path in jar_paths if jar_path not in old_jars]
    if len(jars) == 0:
        # all jars already added
        return
    t_env.get_config().set("pipeline.jars", ";".join([*old_jars, *jars]))
