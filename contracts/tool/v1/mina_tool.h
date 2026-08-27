#ifndef MINA_TOOL_V1_H
#define MINA_TOOL_V1_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MINA_TOOL_ABI_VERSION_V1 1u

/* Borrowed UTF-8 or JSON bytes. The receiver must not retain this pointer. */
typedef struct MinaToolBytesV1 {
    const uint8_t *ptr;
    size_t len;
} MinaToolBytesV1;

typedef void (*MinaToolFreeBytesFnV1)(
    void *plugin_context,
    uint8_t *ptr,
    size_t len
);

/* Plugin-owned bytes transferred to the host. `free` must be non-null. */
typedef struct MinaToolOwnedBytesV1 {
    uint8_t *ptr;
    size_t len;
    MinaToolFreeBytesFnV1 free;
} MinaToolOwnedBytesV1;

/* Transport-level status. Normal tool failures use ToolInvokeResult JSON. */
typedef enum MinaToolAbiStatusV1 {
    MINA_TOOL_ABI_OK_V1 = 0,
    MINA_TOOL_ABI_INVALID_REQUEST_V1 = 1,
    MINA_TOOL_ABI_PLUGIN_FAILURE_V1 = 2
} MinaToolAbiStatusV1;

typedef MinaToolAbiStatusV1 (*MinaToolInvokeFnV1)(
    void *plugin_context,
    MinaToolBytesV1 request_json,
    MinaToolOwnedBytesV1 *result_json_out
);

/* May run concurrently with invoke. Implementations must be thread-safe. */
typedef void (*MinaToolCancelFnV1)(
    void *plugin_context,
    MinaToolBytesV1 call_id_utf8
);

typedef void (*MinaToolDropFnV1)(void *plugin_context);

/*
 * One plugin can expose multiple tools. manifest_json must conform to
 * manifest.schema.json and remain valid until `drop` returns. Execution
 * policy is declared inside that JSON manifest and remains subject to Host
 * validation and policy tightening.
 *
 * struct_size allows a host to ignore fields appended by a future compatible
 * revision. V1 hosts require at least sizeof(MinaToolPluginV1).
 */
typedef struct MinaToolPluginV1 {
    uint32_t abi_version;
    uint32_t struct_size;
    void *plugin_context;
    MinaToolBytesV1 manifest_json;
    MinaToolInvokeFnV1 invoke;
    MinaToolCancelFnV1 cancel;
    MinaToolDropFnV1 drop;
} MinaToolPluginV1;

/*
 * A statically linked library exports an application-chosen symbol with this
 * signature. The embedding host obtains the vtable and registers it explicitly;
 * unique symbol names allow multiple static tool libraries in one executable.
 */
typedef const MinaToolPluginV1 *(*MinaToolPluginEntryFnV1)(void);

#ifdef __cplusplus
}
#endif

#endif /* MINA_TOOL_V1_H */
