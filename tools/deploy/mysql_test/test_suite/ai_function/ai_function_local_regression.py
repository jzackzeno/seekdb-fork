#!/usr/bin/env python3
"""Local seekdb AI-function integration regression tests (Python standard library).

Run from ~/Projects/seekdb-ai-enhancement with the seekdb server running:
  python3 tools/deploy/mysql_test/test_suite/ai_function/ai_function_local_regression.py

Starts a short-lived HTTP mock at 127.0.0.1:18086, creates uniquely named
models/endpoints and a table in ai_assignment_test, and deletes them afterward.
This is an additional local test harness, not a replacement for mysqltest.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 18086
DB = "ai_assignment_test"
ROOT = Path(__file__).resolve()

# Locate obclient using the directory containing .git, without depending on
# the current working directory.
project = next((p for p in [ROOT.parent, *ROOT.parents] if (p / ".git").exists()), None)
if project is None:
    print("ERROR: Put this file inside the seekdb Git repository before running.", file=sys.stderr)
    sys.exit(1)
CLIENT = project / "deps/3rd/u01/obclient/bin/obclient"
if not CLIENT.is_file():
    print(f"ERROR: obclient not found: {CLIENT}", file=sys.stderr)
    sys.exit(1)

request_log = []
request_lock = threading.Lock()


class AIHandler(BaseHTTPRequestHandler):
    def log_message(self, format_string, *args):
        pass

    def do_POST(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(size))
            with request_lock:
                request_log.append((self.path, body))

            if self.path == "/v1/embeddings":
                inputs = body.get("input", [])
                if isinstance(inputs, str):
                    inputs = [inputs]
                data = []
                for idx, content in enumerate(inputs):
                    marker = 101.0 if "Beijing" in content else 202.0
                    if content == "bad-index":
                        data.append({"index": 999, "embedding": [1.0, 0.0, 0.0]})
                    elif content == "bad-vector":
                        data.append({"index": idx, "embedding": [1.0, "not-a-number", 0.0]})
                    else:
                        data.append({"index": idx, "embedding": [marker, 0.0, 0.0]})
                data.reverse()  # Intentionally reverse results to exercise index restoration.
                reply = {"object": "list", "model": body.get("model"), "data": data}

            elif self.path == "/v1/rerank":
                query = str(body.get("query", "")).lower()
                scored = []
                for idx, doc in enumerate(body.get("documents", [])):
                    text = doc if isinstance(doc, str) else str(doc)
                    score = 0.9 if query in text.lower() else 0.1
                    scored.append({"index": idx, "relevance_score": score})
                scored.sort(key=lambda entry: -entry["relevance_score"])
                reply = {"results": scored}

            elif self.path == "/api/embed":
                inputs = body.get("input", [])
                if isinstance(inputs, str):
                    inputs = [inputs]
                reply = {"embeddings": [[0.1, 0.2, 0.3] for _ in inputs]}

            elif self.path == "/api/generate":
                reply = {"response": body.get("prompt", ""), "done": True}

            else:
                self.send_error(404)
                return

            data = json.dumps(reply, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (ValueError, TypeError) as exc:
            self.send_error(400, str(exc))


def sql(statement, expect_error=False):
    cmd = [str(CLIENT), "-c", f"-h{HOST}", "-P2888", "-uroot", f"-D{DB}", "-A", "-N", "-B", "--raw", "-e", statement]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=45)
    output = (proc.stdout + "\n" + proc.stderr).strip()
    if expect_error:
        if proc.returncode == 0:
            raise AssertionError(f"Expected SQL error but query succeeded: {statement}\n{output}")
        if "ERROR" not in output.upper():
            raise AssertionError(f"SQL failed unexpectedly: {output}")
        return output
    if proc.returncode != 0:
        raise AssertionError(f"SQL failed: {statement}\n{output}")
    return proc.stdout.strip()


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def check(name, test):
    test()
    print(f"PASS  {name}", flush=True)


def recent_requests(path, from_index):
    with request_lock:
        return [body for request_path, body in request_log[from_index:] if request_path == path]


def log_position():
    with request_lock:
        return len(request_log)


def create_model(name, model_type, model_name, provider, endpoint_name, url):
    sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(" +
        f"'{name}', '{{\"type\":\"{model_type}\",\"model_name\":\"{model_name}\"}}'" +
        ")")
    created_models.append(name)
    # Ollama deliberately has no API key; other providers use a dummy key.
    key = "" if provider.lower() == "ollama" else "mock-key"
    definition = json.dumps({"ai_model_name": name, "scope": "all", "url": url,
                             "access_key": key, "provider": provider})
    sql(f"CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('{endpoint_name}', '{definition}')")
    created_endpoints.append(endpoint_name)


created_models = []
created_endpoints = []
created_tables = []


def main():
    suffix = str(os.getpid())
    embed = f"reg_embed_{suffix}"
    rerank = f"reg_rerank_{suffix}"
    ollama_embed = f"reg_ollama_embed_{suffix}"
    ollama_complete = f"reg_ollama_complete_{suffix}"
    table = f"reg_ai_embed_rows_{suffix}"

    httpd = ThreadingHTTPServer((HOST, PORT), AIHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    print(f"Started temporary mock server at http://{HOST}:{PORT}")

    passed = 0
    try:
        check("Database connection", lambda: assert_equal(sql("SELECT 1"), "1", "Connection"))
        passed += 1

        create_model(embed, "dense_embedding", "regression-embed", "openai",
                     f"{embed}_ep", f"http://{HOST}:{PORT}/v1/embeddings")
        create_model(rerank, "rerank", "regression-rerank", "siliconflow",
                     f"{rerank}_ep", f"http://{HOST}:{PORT}/v1/rerank")
        create_model(ollama_embed, "dense_embedding", "regression-ollama-embed", "ollama",
                     f"{ollama_embed}_ep", f"http://{HOST}:{PORT}/api/embed")
        create_model(ollama_complete, "completion", "regression-ollama-complete", "OLLAMA",
                     f"{ollama_complete}_ep", f"http://{HOST}:{PORT}/api/generate")

        sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, content TEXT)")
        created_tables.append(table)
        sql(f"INSERT INTO {table} VALUES (1,'Hello Beijing'),(2,'Hello Shanghai')")

        def embedding_reordering():
            start = log_position()
            output = sql(f"SELECT id, AI_EMBED('{embed}', content) FROM {table}")
            rows = [line.split("\t", 1) for line in output.splitlines()]
            assert_equal(len(rows), 2, "Embedding row count")
            mapping = {int(row[0]): json.loads(row[1]) for row in rows}
            assert_equal(mapping[1], [101.0, 0.0, 0.0], "Beijing embedding")
            assert_equal(mapping[2], [202.0, 0.0, 0.0], "Shanghai embedding")
            requests = recent_requests("/v1/embeddings", start)
            assert_equal(len(requests), 1, "HTTP batch request count")
            assert_equal(requests[0]["input"], ["Hello Beijing", "Hello Shanghai"],
                         "HTTP batch input order")
        check("Batch embedding restores out-of-order HTTP results", embedding_reordering)
        passed += 1

        def invalid_embed(content):
            sql(f"SELECT AI_EMBED('{embed}', '{content}')", expect_error=True)
            assert_equal(sql("SELECT 1"), "1", "Database health after rejected embedding")
        check("Embedding rejects out-of-range index safely", lambda: invalid_embed("bad-index"))
        passed += 1
        check("Embedding rejects nonnumeric vector safely", lambda: invalid_embed("bad-vector"))
        passed += 1

        def stable_rerank(num_docs):
            # Build the JSON array in SQL, not hardcoded in the seekdb implementation.
            statement = (
                "SELECT AI_RERANK('" + rerank + "','apple'," +
                f"CONCAT('[',REPEAT('\"banana\",',{num_docs - 1}),'\"banana\"]'))"
            )
            before = log_position()
            ranking = json.loads(sql(statement))
            assert_equal(len(ranking), num_docs, "Reranked document count")
            assert_equal([entry["index"] for entry in ranking], list(range(num_docs)),
                         "Stable global document order")
            requests = recent_requests("/v1/rerank", before)
            expected_counts = [20] * (num_docs // 20)
            if num_docs % 20:
                expected_counts.append(num_docs % 20)
            assert_equal([len(req["documents"]) for req in requests], expected_counts,
                         "Rerank batch sizes")
        check("Stable reranking across 21 documents", lambda: stable_rerank(21))
        passed += 1
        check("Stable reranking across 41 documents", lambda: stable_rerank(41))
        passed += 1

        def null_document_key():
            output = sql(f"SELECT AI_RERANK('{rerank}','apple','[\"banana\",\"apple\"]',NULL)")
            ranking = json.loads(output)
            assert_equal(ranking[0]["index"], 1, "NULL doc key result")
        check("Explicit NULL document key", null_document_key)
        passed += 1

        def object_document_key():
            output = sql(f"SELECT AI_RERANK('{rerank}','apple',"
                         "'[{\"id\":\"a\",\"text\":\"banana\"},"
                         "{\"id\":\"b\",\"text\":\"apple\"}]','text')")
            ranked = json.loads(output)
            assert_equal(ranked[0]["id"], "b", "Object reranking id")
            assert_equal(ranked[0]["_model_score"], 0.9, "Object reranking score")
        check("Object reranking preserves id and adds relevance score", object_document_key)
        passed += 1

        def ollama_embedding():
            result = json.loads(sql(f"SELECT AI_EMBED('{ollama_embed}','hello')"))
            assert_equal(result, [0.1, 0.2, 0.3], "Ollama embedding")
        check("Ollama native embedding", ollama_embedding)
        passed += 1

        def ollama_completion():
            pos = log_position()
            result = sql(f"SELECT AI_COMPLETE('{ollama_complete}','Hello from seekdb!')")
            assert_equal(result, "Hello from seekdb!", "Ollama completion")
            requests = recent_requests("/api/generate", pos)
            assert_equal(len(requests), 1, "Ollama request count")
            assert_equal(requests[0].get("stream"), False, "Ollama non-streaming mode")
        check("Ollama native completion with stream=false", ollama_completion)
        passed += 1

        print(f"\nSUCCESS: {passed} local integration regression checks passed.")
        print("Note: Official competition mysqltest/unittest suites have not been run.")
        return 0
    except (AssertionError, OSError, subprocess.TimeoutExpired, ValueError) as exc:
        print(f"\nFAIL after {passed} passing checks: {exc}", file=sys.stderr)
        return 1
    finally:
        for name in reversed(created_tables):
            try:
                sql(f"DROP TABLE IF EXISTS {name}")
            except Exception as exc:
                print(f"WARNING: Could not delete test table {name}: {exc}", file=sys.stderr)
        for name in reversed(created_endpoints):
            try:
                sql(f"CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('{name}')")
            except Exception as exc:
                print(f"WARNING: Could not delete test endpoint {name}: {exc}", file=sys.stderr)
        for name in reversed(created_models):
            try:
                sql(f"CALL DBMS_AI_SERVICE.DROP_AI_MODEL('{name}')")
            except Exception as exc:
                print(f"WARNING: Could not delete test model {name}: {exc}", file=sys.stderr)
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    sys.exit(main())
