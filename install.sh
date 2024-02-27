#!/usr/bin/env bash

set -e

function install_pyslurm {
    if [ -z "$(which sbatch)" ]; then
        echo "sbatch not found. Please install Slurm first."
        exit 1
    fi
    export SLURM_INCLUDE_DIR=$(dirname $(dirname $(which sbatch)))/include
    export SLURM_LIB_DIR=$(dirname $(dirname $(which sbatch)))/lib
    if [ -d /tmp/pyslurm ]; then
        rm -rf /tmp/pyslurm
    fi
    rm -rf /tmp/pyslurm
    SLURM_VERSION=$(sbatch --version|awk '{print $2}'|awk -F '.' '{print "v"$1"."int($2)}')
    git clone -q https://github.com/PySlurm/pyslurm.git /tmp/pyslurm
    pushd /tmp/pyslurm > /dev/null
    PYSLURM_VERSION=$(git tag --sort=-creatordate --list $SLURM_VERSION'.*'|head -n 1)
    git checkout -q $PYSLURM_VERSION
    sed -i 's/"partition", "partition"/"partition", "partitions"/g' pyslurm/core/job/sbatch_opts.pyx
    pip install .
    popd
    rm -rf /tmp/pyslurm
}

function install_volcengine {
    pip install git+https://github.com/GCS-ZHN/ml-platform-sdk-python@main
}

backend=${1:-"slurm"}

if [ "$backend" == "slurm" ] || [ "$backend" == "all" ]; then
    echo "Installing PySlurm"
    install_pyslurm
elif [ "$backend" == "volcengine" ] || [ "$backend" == "all" ]; then
    echo "Installing VolcEngine"
    install_volcengine
else
    echo "Backend $backend is not supported"
    exit 1
fi

pip install .
