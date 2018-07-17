// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_fdb.h"
#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


fdb_error_t waitError(FDBFuture *f) {
    fdb_error_t blockError = fdb_future_block_until_ready(f);
    if(!blockError) {
        return fdb_future_get_error(f);
    } else {
        return blockError;
    }
}

fdb_error_t checkError(fdb_error_t err, const char* context) {
    return err;
}

void* runNetwork(void* arg) {
    checkError(fdb_run_network(), "run network");
    return nullptr;
}

struct RunResult openDatabase(FDBDatabase** db) {
    fdb_select_api_version(FDB_API_VERSION);
    fdb_error_t err = checkError(fdb_setup_network(), "setup network");
    if (err) {
        return RunResult(err);
    }

    pthread_t netThread;
    pthread_create(&netThread, nullptr, &runNetwork, nullptr);

    FDBFuture *f = fdb_create_cluster(nullptr);
    err = checkError(fdb_future_block_until_ready(f), "block for cluster");
    if (err) {
        return RunResult(err);
    }

    FDBCluster *cluster;
    err = checkError(fdb_future_get_cluster(f, &cluster), "get cluster");
    if (err) {
        return RunResult(err);
    }

    fdb_future_destroy(f);

    f = fdb_cluster_create_database(cluster, (uint8_t*)"DB", 2);

    err = checkError(fdb_future_block_until_ready(f), "block for database");
    if (err) {
        return RunResult(err);
    }

    err = checkError(fdb_future_get_database(f, db), "get database");
    if (err) {
        return RunResult(err);
    }

    fdb_future_destroy(f);
    fdb_cluster_destroy(cluster);

    return RunResult(0);
}

fdb_error_t createTransaction(FDBDatabase* db, FDBTransaction** tr, uint64_t limit, uint64_t timeout) {

    fdb_error_t err = checkError(fdb_database_create_transaction(db, tr), "create transaction");
    if (err) {
        return err;
    }

    err = checkError(fdb_transaction_set_option(*tr, FDB_TR_OPTION_RETRY_LIMIT, (const uint8_t*)&limit, sizeof(uint64_t)), "set retry limit");
    if (err) {
        return err;
    }

    err = checkError(fdb_transaction_set_option(*tr, FDB_TR_OPTION_TIMEOUT, (const uint8_t*)&timeout, sizeof(uint64_t)), "set time out");
    if (err) {
        return err;
    }

    return 0;
}

struct RunResult run(FDBDatabase *db,
		     fdb_error_t (*func)(FDBTransaction*, void*, void*), 
		     void* args,
                     void* result,
                     uint64_t retryLimit,
                     uint64_t timeout)
{
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, retryLimit, timeout);

    if (err) {
        return RunResult(err);
    }

    while(1) {
        err = func(tr, args, result);

        if (!err) {
            FDBFuture *f = fdb_transaction_commit(tr);
            err = waitError(f);
            fdb_future_destroy(f);
        }

        if (err) {
            FDBFuture *f = fdb_transaction_on_error(tr, err);
            fdb_error_t retryE = waitError(f);
            fdb_future_destroy(f);
            if (retryE) 
            {
                fdb_transaction_destroy(tr);
                return RunResult(retryE);
            } else {
                continue;
            }

        }
        break;
    }

    fdb_transaction_destroy(tr);
    return RunResult(0);
}
