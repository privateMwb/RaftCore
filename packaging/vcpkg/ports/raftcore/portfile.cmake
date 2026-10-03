# NOTE: REF values below are placeholders -- fill each with the exact
# commit (the RaftCore commit, and the commit each libs/internal submodule
# points at). SHA512 0 is vcpkg's standard way to get the real hash: run
# the install once, it fails with the actual SHA512 for each download, and
# you paste that in.
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO privateMwb/RaftCore
    REF 5fdfbb30f1a72974750393f71773427bcea77116
    SHA512 8f593c3b3f585b25a489dadbfb2e84b395d27288792358286cb2101d02da357e9b8c48e6094ff520c54452ee3d22cdc79e0142137350478693216d56e51985c6
)

# GitHub archive tarballs never include submodule content, so RaftCore's
# internal libraries under libs/internal/ are fetched separately here,
# each pinned to the exact commit the submodule points at, then copied
# into place.
vcpkg_from_github(
    OUT_SOURCE_PATH FUNCTIONPRO_SOURCE_PATH
    REPO privateMwb/FunctionPro
    REF 11a7bfb414fe6d6e1cb2974905888bf0df0eee4d
    SHA512 155e4420f1221fde79dfaa9bf05a2da5a74dc7a0e2f38470561bc0bd5b86ea0d76c9e739fae7879700457ac4833f11b041551b7cc0ed9d51849244f46971af87
)
vcpkg_from_github(
    OUT_SOURCE_PATH HASHMAPPRO_SOURCE_PATH
    REPO privateMwb/HashMapPro
    REF 122dbff03d3c37f85cc9a734dd0db8d51d888501
    SHA512 a59a2f55f612367cc89c93fa068e9d6beb19de5ee62e094b9a1e272e202dfaec971a41d04cfc007037ccbc8ad12c256933b4293a2115f7d2dc859bc30ba14acf
)
vcpkg_from_github(
    OUT_SOURCE_PATH VECTORPRO_SOURCE_PATH
    REPO privateMwb/VectorPro
    REF 558e1bb9880b13d81ecb195d8ba97d90298fadfc
    SHA512 ffc53977d22a052e7f20d8dbab5869b33cb52dd28e4f5ab3b8a15772de1ba106bed9d04f294b78b317325151152b6d3f98938d23b266acaa72ef3a5badbb61c2
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
