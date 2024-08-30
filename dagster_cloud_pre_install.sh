#!/bin/bash
set -x  # Echo all commands for debugging

apt-get update
apt-get install -y \
  build-essential \
  g++ \
  libffi-dev \
  libssl-dev \
  python3-dev
