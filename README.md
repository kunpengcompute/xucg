# UCG
Group-based Collective Operations for UCX

# Build & Install
1. `cd xucg`
2. `mkdir build && cd build`
3. `cmake -DCMAKE_INSTALL_PREFIX=${install_path} -DCMAKE_BUILD_TYPE=Release -DUCG_BUILD_WITH_UCX=${ucx_path} ..`
> example
> - build debug version: `cmake -DCMAKE_INSTALL_PREFIX=${install_path} -DCMAKE_BUILD_TYPE=Debug -DUCG_BUILD_WITH_UCX=${ucx_path} ..`
> - build planc hccl: `cmake -DCMAKE_INSTALL_PREFIX=${install_path} -DCMAKE_BUILD_TYPE=Debug -DUCG_BUILD_WITH_UCX=${ucx_path} -DUCG_BUILD_WITH_HCCL=${hccl_path} -DUCG_BUILD_PLANC_HCCL=ON ..`
4. `make -j && make install`

# UCG Build Options
- UCG_BUILD_TOOLS: Build UCG tools, default ON
- UCG_BUILD_TESTS: Build UCG tests, default ON
- UCG_BUILD_WITH_UCX: Specify the UCX install path
- UCG_BUILD_WITH_HCCL: Specify the HCCL install path

- UCG_ENABLE_PROFILE: Enable profiling, default OFF
- UCG_ENABLE_GCOV: Enable code coverage, default OFF
- UCG_ENABLE_CHECK_PARAMS: Enable checking parameters, default ON
- UCG_ENABLE_MT: Enable thread-safe support, default OFF

## PlanC UCX Build Options
- UCG_BUILD_PLANC_UCX: Build the plan component which is based on UCX, default ON

## PlanC HCCL Build Options
- UCG_BUILD_PLANC_HCCL: Build the plan component which is based on HCCL, default OFF

# UCG PlanM Usage
If you want to load a closed-source module, placing the closed-source library to `${ucg_install_path}/lib/planc`.