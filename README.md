# doris-data-generator-go

`doris-data-generator-go` is a utility for Apache Doris / SelectDB test data workflows. It can generate synthetic log data from a Doris table DDL, write Parquet files, upload files to OSS/S3-compatible storage, import generated Parquet files through Doris S3 TVF, and copy data between Doris tables by partition or tablet.

## Features

- Generate rows from a Doris `CREATE TABLE` DDL.
- Configure per-field generation rules with JSON.
- Generate Parquet files with configurable compression.
- Split generated Parquet files by log type suffix, for example `*.nginx_access.parquet` and `*.json_log_large.parquet`.
- Upload generated files to Aliyun OSS / S3-compatible object storage.
- Import Parquet files from OSS/S3 into Doris by S3 TVF.
- Import Parquet files from S3-compatible object storage into Doris through concurrent Stream Load.
- Filter TVF import by generated log type suffix.
- Remap string fields to integer values during TVF import or table copy.
- Copy Doris table data by partition or tablet.
- Optional cluster routing with `USE @cluster`.

## Requirements

- Go `1.24.x` or newer.
- Network access for Go module download.
- `mysql` client in `PATH` when using Doris table copy mode.
- Doris FE MySQL port for copy mode, usually `9030`.
- Doris FE HTTP port for Stream Load, usually `8030`.
- OSS/S3 credentials when upload or TVF import is enabled.

If Go dependencies need a proxy, set it before building:

```bash
export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890
export all_proxy=socks5://127.0.0.1:7890
```

## Build

Build for the current machine:

```bash
go mod tidy
go build -o doris-data-generator .
```

Build a Linux x86_64 binary from macOS:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o dist/doris-data-generator-linux-amd64 .
```

Build a macOS Apple Silicon binary:

```bash
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o dist/doris-data-generator-darwin-arm64 .
```

Run tests:

```bash
go test ./...
```

## Generate Parquet Files

Prepare a Doris DDL file, for example `1.sql`, and a generation config, for example `config.json`.

Example `config.json`:

```json
{
  "datetime_range": ["2026-03-15 00:00:00", "2026-03-26 23:59:59"],
  "low_cardinality": {
    "log_level": true,
    "_namespace_": true,
    "app": true
  },
  "fields": {
    "msg": {
      "log_types": [
        ["nginx_access", 0.7],
        ["json_log_large", 0.3]
      ],
      "chinese_ratio": 0.3
    },
    "log_level": {
      "values": ["INFO", "WARN", "ERROR", "DEBUG"]
    },
    "_namespace_": {
      "values": [
        ["kube-system", 0.9],
        ["opentelemetry", 0.01],
        ["maxclaw", 0.01],
        ["sandbox-system", 0.01],
        ["mon-stack", 0.01],
        ["flux-system", 0.01],
        ["infra", 0.01],
        ["idp-system-appupgrade", 0.01],
        ["weaver", 0.01],
        ["mlogs", 0.01]
      ]
    },
    "app": {
      "values": [
        ["matrix-agent-manager", 0.6],
        ["matrix-sandbox-file-gateway", 0.016],
        ["weaver-llm-gateway-claw", 0.016],
        ["otel-agent", 0.016],
        ["notification-controller", 0.016],
        ["helm-controller", 0.016]
      ]
    }
  }
}
```

Generate local Parquet files:

```bash
./doris-data-generator \
  --ddl-file ./1.sql \
  --config-file ./config.json \
  --rows 10000000 \
  --chunksize 500000 \
  --parallel 16 \
  --writer-parallel 8 \
  --compression zstd \
  --output /tmp/ali_virginia_parquet \
  --no-upload
```

Notes:

- `datetime_range` is used for datetime-like columns such as `_ctime_`; generated values include microseconds.
- When `msg.log_types` contains multiple log types, files are split by suffix. A file name may look like `data_20260507_145536.0049.nginx_access.parquet`.
- If only one log type is needed, you can write `log_types` as a plain string like `"json_log_large"`, or as one-item array `[["json_log_large", 1.0]]`.
- Larger `--chunksize` usually produces larger Parquet files.
- `--writer-parallel` controls Parquet writer concurrency.
- `--upload-parallel` controls OSS upload concurrency when upload is enabled.

## Generate And Upload To OSS

```bash
./doris-data-generator \
  --ddl-file ./1.sql \
  --config-file ./config.json \
  --rows 10000000 \
  --chunksize 500000 \
  --parallel 16 \
  --writer-parallel 8 \
  --upload-parallel 8 \
  --compression zstd \
  --output /tmp/ali_virginia_parquet \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$OSS_AK" \
  --oss-sk "$OSS_SK" \
  --oss-addressing-style virtual-host \
  --cleanup
```

For Aliyun OSS, prefer virtual-host style unless the target endpoint explicitly requires path style.

## Direct Stream Load To Doris

Use `--no-parquet` together with Doris connection options to generate data and send it directly to Doris through concurrent Stream Load requests. In this mode, generated rows are sent through a bounded in-memory pipeline; the tool no longer keeps a full output chunk in memory before loading.

```bash
./doris-data-generator \
  --ddl-file ./1.sql \
  --config-file ./config.json \
  --rows 10000000 \
  --parallel 16 \
  --stream-load-parallel 8 \
  --pipeline-buffer 2 \
  --doris-batch-size 10000 \
  --doris-host 127.0.0.1 \
  --doris-port 8030 \
  --doris-database minimax \
  --doris-table ali_virginia_prod_01_17 \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD" \
  --group-commit \
  --no-parquet
```

Parameter guidance:

- `--parallel` controls data generation workers.
- `--stream-load-parallel` controls concurrent Stream Load requests. If omitted, it defaults to `--parallel`.
- `--ordered-stream-load` forces one generator and one Stream Load worker so generated batches are loaded in timestamp offset order.
- `--doris-batch-size` controls rows per Stream Load request.
- `--pipeline-buffer` controls how many generated batches can wait in memory per worker group.
- `--doris-port` is the Doris FE HTTP port, usually `8030`, not the MySQL port `9030`.
- `--group-commit` enables Doris group commit with `group_commit=async_mode`.
- `--debug` prints Stream Load HTTP status and response body for each batch.

## TVF Import From OSS/S3

Import only one generated log type into a target table:

```bash
./doris-data-generator \
  --tvf-import \
  --tvf-log-type json_log_large \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$OSS_AK" \
  --oss-sk "$OSS_SK" \
  --doris-host 127.0.0.1 \
  --doris-port 9030 \
  --doris-database minimax \
  --doris-table json_log_large_table \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD" \
  --batch-files 10 \
  --parallel 4
```

Preview generated SQL without executing:

```bash
./doris-data-generator \
  --tvf-import \
  --tvf-log-type nginx_access \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$OSS_AK" \
  --oss-sk "$OSS_SK" \
  --doris-host 127.0.0.1 \
  --doris-port 9030 \
  --doris-database minimax \
  --doris-table ali_virginia_prod_01_17 \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD" \
  --batch-files 10 \
  --parallel 4 \
  --dry-run
```

If `--tvf-log-type` is omitted, all Parquet files under `--oss-path` are imported.

## Stream Load From S3-Compatible Storage

Use this mode when generated Parquet files already exist in OSS, AWS S3, MinIO, Ceph RGW, or another S3-compatible object store, and you want to benchmark Doris Stream Load independently from data generation.

```bash
./doris-data-generator \
  --s3-import \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$S3_AK" \
  --oss-sk "$S3_SK" \
  --oss-addressing-style virtual-host \
  --s3-region cn-shanghai \
  --tvf-log-type json_log_large \
  --doris-host 127.0.0.1 \
  --doris-port 8030 \
  --doris-database minimax \
  --doris-table json_log_large_table \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD" \
  --parallel 16 \
  --ordered-stream-load
```

Notes:

- `--s3-import` reads Parquet files from S3-compatible storage and sends each file as a Doris Stream Load request with `format=parquet`.
- `--parallel` controls concurrent object downloads and Stream Load requests.
- `--ordered-stream-load` imports sorted Parquet object keys one by one. Use it when files are generated in chronological filename order and Doris must receive them in that same order.
- `--tvf-log-type` can be reused as a file suffix filter, for example `json_log_large` matches `*.json_log_large.parquet`.
- `--oss-*` parameters are kept for compatibility; in this mode they mean S3-compatible bucket, prefix, endpoint, access key, and secret key.
- `--s3-region` is required for AWS Signature V4 signing. For MinIO or other S3-compatible services, use the region configured by that service, often `us-east-1`.
- Do not pass `--group-commit` unless you explicitly want `group_commit=async_mode`.

Run TVF import in a Doris compute cluster:

```bash
./doris-data-generator \
  --tvf-import \
  --tvf-cluster doris_cluster \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$OSS_AK" \
  --oss-sk "$OSS_SK" \
  --doris-host 127.0.0.1 \
  --doris-port 9030 \
  --doris-database minimax \
  --doris-table target_table \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD"
```

This emits `USE @doris_cluster` before insert execution.

## Remap String Field During TVF Import

Example: convert `app` string values to integers starting from `100001`:

```bash
./doris-data-generator \
  --tvf-import \
  --tvf-log-type json_log_large \
  --tvf-remap-string app \
  --tvf-remap-string-values 'matrix-agent-manager:100001,matrix-sandbox-file-gateway:100002,weaver-llm-gateway-claw:100003,otel-agent:100004,notification-controller:100005,helm-controller:100006' \
  --oss-bucket minimax-selectdb-test \
  --oss-path /doris/generated/logtest/ \
  --oss-endpoint oss-cn-shanghai-internal.aliyuncs.com \
  --oss-ak "$OSS_AK" \
  --oss-sk "$OSS_SK" \
  --doris-host 127.0.0.1 \
  --doris-port 9030 \
  --doris-database minimax \
  --doris-table target_table \
  --doris-user root \
  --doris-password "$DORIS_PASSWORD"
```

## Copy Doris Table Data

Copy table `A` to table `B` by partition:

```bash
./doris-data-generator \
  --database minimax \
  --source-table ali_virginia_prod_01_09 \
  --target-table ali_virginia_prod_01_15 \
  --host 127.0.0.1 \
  --port 9030 \
  --user root \
  --password "$DORIS_PASSWORD" \
  --copy-mode partition \
  --parallel 8
```

Copy by tablet:

```bash
./doris-data-generator \
  --database minimax \
  --source-table source_table \
  --target-table target_table \
  --host 127.0.0.1 \
  --port 9030 \
  --user root \
  --password "$DORIS_PASSWORD" \
  --copy-mode tablet \
  --parallel 8
```

Preview copy SQL:

```bash
./doris-data-generator \
  --database minimax \
  --source-table source_table \
  --target-table target_table \
  --copy-mode partition \
  --dry-run
```

Resume copy from log:

```bash
./doris-data-generator \
  --database minimax \
  --source-table source_table \
  --target-table target_table \
  --copy-mode partition \
  --resume \
  --log-file partition_copy.log
```

## Common Parameters

- `--rows`: total generated rows.
- `--chunksize`: rows per chunk/file group.
- `--file-size`: target file size such as `128MB` or `1GB`; mutually exclusive with explicit `--partitions`.
- `--parallel`: generator or execution concurrency.
- `--stream-load-parallel`: concurrent Doris Stream Load workers for `--no-parquet` direct loading.
- `--writer-parallel`: Parquet writer concurrency.
- `--upload-parallel`: OSS upload concurrency.
- `--pipeline-buffer`: number of generated chunks allowed to wait for writers.
- `--compression`: Parquet compression, for example `snappy` or `zstd`.
- `--cleanup`: delete local files after successful upload.
- `--no-upload`: generate local files only.
- `--dry-run`: preview SQL for copy or TVF import mode.

## Safety Notes

- Do not pass real credentials directly in shell history. Prefer environment variables such as `$OSS_AK`, `$OSS_SK`, and `$DORIS_PASSWORD`.
- TVF import and copy mode execute `INSERT INTO ... SELECT ...`; validate target tables before running large jobs.
- For large generation jobs on a 16C/64G machine, a practical starting point is `--parallel 16 --writer-parallel 8 --upload-parallel 8 --chunksize 500000`.
