if(NOT DEFINED WORKER OR NOT DEFINED TEST_ROOT_PARENT)
  message(FATAL_ERROR "WORKER and TEST_ROOT_PARENT are required")
endif()

string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef nonce)
set(test_root "${TEST_ROOT_PARENT}/snapshot-transfer-${nonce}")
file(MAKE_DIRECTORY "${test_root}")

foreach(phase write coordinate read)
  execute_process(
    COMMAND "${WORKER}" "${phase}" "${test_root}"
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(NOT status EQUAL 0)
    file(REMOVE_RECURSE "${test_root}")
    message(FATAL_ERROR "${phase} failed (${status}):\n${output}${error}")
  endif()
endforeach()

file(REMOVE_RECURSE "${test_root}")
