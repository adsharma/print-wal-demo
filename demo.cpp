// demo.cpp — drives the print_wal extension.
//
//  1. Creates (or reopens) an on-disk database.
//  2. Loads the print_wal extension.
//  3. CALL print_wal_start(): opens a long-running read-only transaction that
//     pins the WAL (checkpoints can no longer rotate/retire it) and starts a
//     publisher thread that tails + prints WAL records.
//  4. Runs DDL (CREATE NODE/REL TABLE) and DML (CREATE/MATCH/SET/DELETE, COPY,
//     DDL like ALTER) so the publisher has records to print.
//  5. CALL print_wal_status() / print_wal_stop().
//
// Build:  cmake -B /tmp/print_wal/build -S /tmp/print_wal && cmake --build /tmp/print_wal/build -j
// Run:    /tmp/print_wal/build/demo

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <thread>

#include "main/connection.h"
#include "main/database.h"

using namespace lbug::main;

static void exec(Connection& conn, const std::string& query) {
    std::cout << ">>> " << query << std::endl;
    auto result = conn.query(query);
    if (!result->isSuccess()) {
        std::cout << "!! FAILED: " << result->getErrorMessage() << std::endl;
        return;
    }
    // Print single-row CALL results so start/status/stop output is visible.
    std::cout << result->toString();
}

int main() {
    const std::string dbPath = "/tmp/print_wal/demo_db";
    const std::string extPath = "/tmp/print_wal/build/libprint_wal.lbug_extension";
    std::cout << "db: " << dbPath << "\next: " << extPath << std::endl;

    auto db = std::make_unique<Database>(dbPath);
    auto conn = std::make_unique<Connection>(db.get());

    exec(*conn, "LOAD EXTENSION '" + extPath + "';");

    // 1. Pin the WAL + start the publisher thread.
    exec(*conn, "CALL print_wal_start() RETURN *;");

    // 2. DDL: records of type CREATE_CATALOG_ENTRY_RECORD.
    exec(*conn, "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY(name));");
    exec(*conn, "CREATE NODE TABLE Post(title STRING, likes INT64, PRIMARY KEY(title));");
    exec(*conn, "CREATE REL TABLE Likes(FROM User TO Post, since INT64);");

    // Give the publisher a moment so DDL records print before DML ones.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 3. DML: TABLE_INSERTION_RECORD rows.
    exec(*conn, "CREATE (:User {name: 'Alice', age: 30});");
    exec(*conn, "CREATE (:User {name: 'Bob', age: 25});");
    exec(*conn, "CREATE (:Post {title: 'Hello', likes: 10});");
    exec(*conn, "MATCH (a:User), (b:Post) WHERE a.name = 'Alice' AND b.title = 'Hello' "
                "CREATE (a)-[:Likes {since: 2024}]->(b);");

    // 4. More DML flavors: update + delete (NODE_UPDATE / NODE_DELETION ...).
    exec(*conn, "MATCH (u:User) WHERE u.name = 'Bob' SET u.age = 26 RETURN u.name, u.age;");
    exec(*conn, "MATCH (u:User) WHERE u.name = 'Bob' DELETE u;");

    // 5. Checkpoint while pinned: it must NOT retire the WAL (the publisher
    //    keeps reading old offsets). Force one explicitly.
    exec(*conn, "CHECKPOINT;");
    exec(*conn, "CALL print_wal_status() RETURN *;");

    // 6. Writes after the pinned checkpoint still get published.
    exec(*conn, "CREATE (:User {name: 'Carol', age: 41});");

    std::this_thread::sleep_for(std::chrono::seconds(2));
    exec(*conn, "CALL print_wal_status() RETURN *;");
    exec(*conn, "CALL print_wal_stop() RETURN *;");

    std::cout << "demo done" << std::endl;
    return 0;
}
