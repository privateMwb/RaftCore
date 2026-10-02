#!/bin/bash -eu
# ============================================================
# .clusterfuzzlite/build.sh
#
# RaftCore isn't header-only, so unlike a harness that compiles
# straight against headers, this first compiles every library
# translation unit under src/RaftCore/ once (with the sanitizer flags
# ClusterFuzzLite passes in $CXXFLAGS), then links each
# fuzz/fuzz_*.cpp harness against those objects.
#
# Harnesses are discovered by glob: drop a new fuzz/fuzz_<target>.cpp
# in place and it becomes its own $OUT binary with no edit here.
#
# RaftCore's own libraries (FunctionPro, HashMapPro, VectorPro, ...)
# live under libs/ -- every directory named `include` below it is
# added to the include path.
# ============================================================

cd "${SRC}/RaftCore"

INCLUDES=(-I"${SRC}/RaftCore/include")
while IFS= read -r dir; do
  INCLUDES+=(-I"${dir}")
done < <(find libs -type d -name include 2>/dev/null)

# Library sources, compiled once and shared by every harness.
OBJECTS=()
index=0
while IFS= read -r source; do
  object="${WORK}/raftcore_${index}.o"
  $CXX $CXXFLAGS -std=c++20 "${INCLUDES[@]}" -c "${source}" -o "${object}"
  OBJECTS+=("${object}")
  index=$((index + 1))
done < <(find src/RaftCore -name '*.cpp' ! -name 'main.cpp')

# One binary per harness.
for harness in fuzz/fuzz_*.cpp; do
  name="$(basename "${harness}" .cpp)"
  $CXX $CXXFLAGS -std=c++20 "${INCLUDES[@]}" \
    "${harness}" \
    "${OBJECTS[@]}" \
    $LIB_FUZZING_ENGINE \
    -o "${OUT}/${name}"
done
