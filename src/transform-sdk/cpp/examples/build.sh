#!/bin/env bash

docker run -v `pwd`/..:/src -w /src/examples ghcr.io/webassembly/wasi-sdk /bin/bash -c 'cmake -Bbuild && cmake --build build'
