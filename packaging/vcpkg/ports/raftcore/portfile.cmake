# NOTE: REF values below are placeholders -- fill each with the exact
# commit (the RaftCore commit, and the commit each libs/internal submodule
# points at). SHA512 0 is vcpkg's standard way to get the real hash: run
# the install once, it fails with the actual SHA512 for each download, and
# you paste that in.
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO privateMwb/RaftCore
    REF <commit-sha>
    SHA512 0
)

# GitHub archive tarballs never include submodule content, so RaftCore's
# internal libraries under libs/internal/ are fetched separately here,
# each pinned to the exact commit the submodule points at, then copied
# into place.
vcpkg_from_github(
    OUT_SOURCE_PATH FUNCTIONPRO_SOURCE_PATH
    REPO privateMwb/FunctionPro
    REF <commit-sha>
    SHA512 0
)
vcpkg_from_github(
    OUT_SOURCE_PATH HASHMAPPRO_SOURCE_PATH
    REPO privateMwb/HashMapPro
    REF <commit-sha>
    SHA512 0
)
vcpkg_from_github(
    OUT_SOURCE_PATH VECTORPRO_SOURCE_PATH
    REPO privateMwb/VectorPro
    REF <commit-sha>
    SHA512 0
)

foreach(SUBMODULE_NAME FunctionPro HashMapPro VectorPro)
    file(REMOVE_RECURSE "${SOURCE_PATH}/libs/internal/${SUBMODULE_NAME}")
endforeach()
file(RENAME "${FUNCTIONPRO_SOURCE_PATH}" "${SOURCE_PATH}/libs/internal/FunctionPro")
file(RENAME "${HASHMAPPRO_SOURCE_PATH}" "${SOURCE_PATH}/libs/internal/HashMapPro")
file(RENAME "${VECTORPRO_SOURCE_PATH}" "${SOURCE_PATH}/libs/internal/VectorPro")

set(VCPKG_PORT_NAME RaftCore)

# Consumers only need the library itself, not the tests, benchmarks,
# regression tools, or examples.
vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_TESTS=OFF
        -DBUILD_BENCHMARKS=OFF
        -DBUILD_REGRESSION=OFF
        -DBUILD_EXAMPLES=OFF
)

vcpkg_cmake_install()

vcpkg_cmake_config_fixup(
    PACKAGE_NAME ${VCPKG_PORT_NAME}
    CONFIG_PATH lib/cmake/${VCPKG_PORT_NAME}
)

# This library is compiled (not header-only), so debug binaries are
# real and must be kept — only the duplicate debug/include headers
# are removed.
file(
    REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/debug/include"
)

vcpkg_install_copyright(
    FILE_LIST "${SOURCE_PATH}/LICENSE"
)
