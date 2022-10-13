#
# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The script runs all the quickstarts.

set -e

CURRENT_DIR=$(dirname "${BASH_SOURCE-$0}")
PROJECT_DIR=$(cd "${CURRENT_DIR}/../.."; pwd)

FLINK_VERSION=1.15.2

cd "${PROJECT_DIR}"

python -m pip install ./wheels/*

echo "Running local processor quickstart."
python python/feathub/examples/nyc_taxi.py

echo "Downloading Flink."
curl -LO https://archive.apache.org/dist/flink/flink-"${FLINK_VERSION}"/flink-"${FLINK_VERSION}"-bin-scala_2.12.tgz
tar -xzf flink-"${FLINK_VERSION}"-bin-scala_2.12.tgz

echo "Starting standalone Flink cluster."
./flink-"${FLINK_VERSION}"/bin/start-cluster.sh
echo "Running Flink processor session mode quickstart."
python python/feathub/examples/nyc_taxi_flink_session.py

echo "Restarting standalone Flink cluster."
./bin/stop-cluster.sh && sleep 2 && ./bin/start-cluster.sh
echo "Running Flink processor cli mode quickstart."
bash tools/cli-deps/build-cli-deps.sh
./flink-"${FLINK_VERSION}"/bin/flink run \
    --detach \
    --python python/feathub/examples/streaming_average_flink_cli.py \
    --pyFiles tools/cli-deps/deps.zip

# Wait until there are at least 10 output from TM stdout.
while true; do
  if [ "$(cat ./flink-"${FLINK_VERSION}"/log/*taskexecutor*.out | wc -l)" -ge 10 ]; then
    cat ./flink-"${FLINK_VERSION}"/log/*taskexecutor*.out
    break
  fi
  sleep 5
done

echo "Stopping standalone Flink cluster."
./flink-"${FLINK_VERSION}"/bin/stop-cluster.sh