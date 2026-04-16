FROM --platform=linux/amd64 ubuntu:24.04

RUN apt update && apt install -y --no-install-recommends \
    wget \
    ca-certificates \
    git \
    make \
    cmake \
    python3 \
    zsh \
    gdb \
    ninja-build \
    libidn11-dev \
    ragel \
    yasm \
    libc-ares-dev \
    libre2-dev \
    rapidjson-dev \
    zlib1g-dev \
    libxxhash-dev \
    libzstd-dev \
    libsnappy-dev \
    libgtest-dev \
    libgmock-dev \
    libbz2-dev \
    liblz4-dev \
    libdouble-conversion-dev \
    libssl-dev \
    libstdc++-13-dev \
    lsb-release \
    software-properties-common \
    libidn2-0-dev \
    gcc-13 \
    g++-13 \
    ccache \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/ccache
ENV CCACHE_MAXSIZE=10G

# Install clang-16 and set as default
RUN wget https://apt.llvm.org/llvm.sh && \
    chmod u+x llvm.sh && \
    ./llvm.sh 16 && \
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-16 160 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-16 160 && \
    update-alternatives --install /usr/bin/llvm-config llvm-config /usr/bin/llvm-config-16 160

# Install abseil-cpp
RUN wget -O abseil-cpp-20230802.0.tar.gz https://github.com/abseil/abseil-cpp/archive/refs/tags/20230802.0.tar.gz && \
    tar -xvzf abseil-cpp-20230802.0.tar.gz && \
    cd abseil-cpp-20230802.0 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DABSL_PROPAGATE_CXX_STD=ON -DCMAKE_CXX_FLAGS="-fPIC" .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../

# install protobuf
RUN wget -O protobuf-3.21.12.tar.gz https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.21.12.tar.gz && \
    tar -xvzf protobuf-3.21.12.tar.gz && \
    cd protobuf-3.21.12 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Release \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_INSTALL=ON \
    -DCMAKE_CXX_FLAGS="-fPIC" .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../

# Install gRPC
RUN wget -O grpc-1.54.3.tar.gz https://github.com/grpc/grpc/archive/refs/tags/v1.54.3.tar.gz && \
    tar -xvzf grpc-1.54.3.tar.gz && cd grpc-1.54.3 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local/ \
    -DCMAKE_PREFIX_PATH=/usr/local/ \
    -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package \
    -Dabsl_DIR=/usr/local/lib/cmake/absl \
    -DProtobuf_ROOT=/usr/local/ .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../ 

# Install base64
RUN wget -O base64-0.5.2.tar.gz https://github.com/aklomp/base64/archive/refs/tags/v0.5.2.tar.gz && \
    tar -xvzf base64-0.5.2.tar.gz && cd base64-0.5.2 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-fPIC" .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../ 

# Install brotli
RUN wget -O brotli-1.1.0.tar.gz https://github.com/google/brotli/archive/refs/tags/v1.1.0.tar.gz && \
    tar -xvzf brotli-1.1.0.tar.gz && cd brotli-1.1.0 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-fPIC" .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../

# Install jwt-cpp
RUN wget -O jwt-cpp-0.7.0.tar.gz https://github.com/Thalhammer/jwt-cpp/archive/refs/tags/v0.7.0.tar.gz && \
    tar -xvzf jwt-cpp-0.7.0.tar.gz && cd jwt-cpp-0.7.0 && \
    rm -rf build && mkdir build && cd build && \
    cmake -G Ninja -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-fPIC" .. && \
    cmake --build . --config Release && \
    cmake --install . --config Release --prefix=/usr/local/ && \
    cd ../../

# build the project
RUN wget -O ydb-cpp-sdk.tar.gz https://github.com/ydb-platform/ydb-cpp-sdk/archive/refs/tags/v3.17.0.tar.gz && \
    tar -xvzf ydb-cpp-sdk.tar.gz && cd ydb-cpp-sdk && \
    rm -rf build && cmake --preset release-clang \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DYDB_SDK_INSTALL=On && \
    cmake --build --preset default

# Install Oh My Zsh
RUN sh -c "$(wget --no-check-certificate -O- https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended && \
    git clone https://github.com/zsh-users/zsh-autosuggestions ~/.oh-my-zsh/custom/plugins/zsh-autosuggestions
ENV SHELL=/bin/zsh

