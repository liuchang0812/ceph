#pragma once

#include <pthread.h>

// fdb headers
#define FDB_API_VERSION 510
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

const uint64_t MAX_RETEY = 0xffffffffffffffff;
const uint64_t MAX_TIMEOUT = 5 * 1000;

fdb_error_t waitError(FDBFuture *f);

fdb_error_t checkError(fdb_error_t err, const char* context);

struct RunResult {
    fdb_error_t err_code;
    const char* err_msg;
    RunResult(fdb_error_t err) : err_code(err) {
        err_msg = fdb_get_error(err);
    }
};

void* runNetwork(void* arg);

struct RunResult openDatabase(FDBDatabase** db);

fdb_error_t createTransaction(FDBDatabase* db, FDBTransaction** tr, uint64_t limit, uint64_t timeout);

struct RunResult run(FDBDatabase *db,
                     fdb_error_t (*func)(FDBTransaction*, void*, void*),
                     void* args,
                     void* result,
                     uint64_t retryLimit = MAX_RETEY,
                     uint64_t timeout = MAX_TIMEOUT);
