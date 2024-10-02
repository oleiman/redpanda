v_cc_library(
  NAME security_audit
  SRCS
    audit_log_manager.cc
    probes.cc
    logger.cc
    rpc_client.cc
    schemas/utils.cc
    schemas/hashing_utils.cc
  DEPS
    v::kafka_client
    v::bytes
    v::utils
    v::config
    v::container
)

add_subdirectory(schemas)
add_subdirectory(tests)
