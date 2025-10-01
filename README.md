# Merge ZIP Suite (Go · Python · Node.js · Rust)

Tools to merge many **large** `.zip` files into **one** `.zip` with:
- streaming I/O (low RAM),
- per-zip and overall **% progress**,
- **Elapsed** and **ETA**,
- keep original **root paths** (no extra nested folder),
- auto de-duplicate duplicates: `__dupN`,
- optional `--prefix-by-zip` to restore the old behavior,
- optional **raw split** of the final output (`*.zip.part-000`...),
- (Python/Bash script also supports multi-part ZIP via `zip -s` if the `zip` CLI is installed).

> Designed for folders where each input file (and the merged result) can be multiple GB.

## Structure
```
merge-zip-suite/
  README.md
  go/main.go
  python/merge_zip.py
  nodejs/package.json
  nodejs/index.mjs
  rust/Cargo.toml
  rust/src/main.rs
```

## Quick Start

### Go
```bash
cd go
go build -o mergezip_go ./main.go
# Mặc định output: <input>_output/<out>.zip
./mergezip_go -input ../samples -out merged
# => tạo: ../samples_output/merged.zip
```

### Python
```bash
cd python
# Mặc định output: <input>_output/<out>.zip
python3 merge_zip.py ../samples merged
# => tạo: ../samples_output/merged.zip
```

### Node.js
```bash
cd nodejs
npm i
# Mặc định output: <input>_output/<out>.zip
node index.mjs --input ../samples --out merged
# => tạo: ../samples_output/merged.zip
```

### Rust
```bash
cd rust
cargo build --release
# Mặc định output: <input>_output/<out>.zip
./target/release/mergezip_rs --input ../samples --out merged
# => tạo: ../samples_output/merged.zip
```

## Notes
- **Keep root by default**. Use `--prefix-by-zip` if you *do* want `<zipname>/...` wrapping.
- **GPU is unnecessary** for this I/O-bound workflow. The fastest path is avoiding recompression (store).
- RAW split is implemented in all languages; ZIP split (multi-part .z01) is easiest via `zip -s` CLI.
- Tested with Go 1.20+, Python 3.9+, Node 18+, Rust 1.77+ (zip crate 0.6).

## Performance & Benchmarks

**How time is estimated**

- Total time ≈ **Total uncompressed bytes** ÷ **effective compression throughput (MB/s)**.
- The tools pre-scan inputs and print progress like:  
  `Overall: 37% (12.1 GB/**32.4 GB**)` → the second value (**32.4 GB**) is the total **uncompressed** size used for ETA.

**Typical single-thread Deflate throughput** (modern CPUs, NVMe; no throttling):
- **Node.js** (zlib C) & **Python** (`zlib` C): ~**60–120 MB/s**
- **Rust** (`zip` crate, miniz/zlib): ~**50–110 MB/s**
- **Go** (`compress/flate`): ~**40–90 MB/s**

**Example**  
Final ZIP **13.3 GB** with ~**2:1** ratio ⇒ total uncompressed ≈ **26.6 GB**.

- 120 MB/s → ~**3.8 min**
- 110 MB/s → ~**4.1 min**
- 90  MB/s → ~**5.0 min**
- 60  MB/s → ~**7.6 min**
- 40  MB/s → ~**11.3 min**

Putting it together (including overheads like file enumeration and small files):
- **Node.js / Python:** ~**4.5–10 min**
- **Rust:** ~**5–10 min**
- **Go:** ~**6–13 min**

> Notes
> - `--store` (no recompression) can be bounded by disk speed; output will be larger.
> - Avoiding recompression (raw copy of compressed streams) is fastest but requires low-level ZIP writing and is not enabled by default here.
> - Ensure free space ≥ **1.1×** the intended final ZIP to avoid `No space left on device`.

## Root-level wrappers

Ở thư mục gốc có sẵn các script chạy nhanh (giả định có thư mục `Raw` cạnh bộ mã nguồn):
```bash
./merge_python.sh   # tạo Raw_output/merged_python.zip
./merge_go.sh       # tạo Raw_output/merged_go.zip
./merge_node.sh     # tạo Raw_output/merged_nodejs.zip
./merge_rust.sh     # tạo Raw_output/merged_rust.zip
```
Bạn vẫn có thể truyền thêm tuỳ chọn như `--store`, `--chunk-mb`, `--filter 'part-*.zip'`, `--split-size 1900m`, v.v.


## Disk space pre-check

Cả 4 phiên bản sẽ **ước lượng dung lượng cần** trước khi ghi:
- `--store`: cần ≈ **1.05 × tổng dữ liệu không nén**.
- `deflate` (mặc định): cần ≈ **min(tổng không nén, 1.25×tổng nén nguồn) × 1.10**.
- Nếu thiếu dung lượng, chương trình dừng sớm (exit code `8`) và in thông báo chi tiết (GB).
