#! /bin/env bash

docker run -v `pwd`/..:/src -w /src/cpp/build xsdk/cpp /bin/bash -c "apt update && apt install -y clang cmake && cmake  -DREDPANDA_TRANSFORM_SDK_ENABLE_TESTING=ON ..  && cmake --build . && ./test_binary"
