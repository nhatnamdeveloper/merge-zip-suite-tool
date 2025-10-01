#!/usr/bin/env python3
import argparse, os, sys, time, fnmatch, shutil, shutil, zipfile, math, subprocess

def human(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    n = float(n)
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.1f} {u}"
        n /= 1024

def hms(sec: float) -> str:
    sec = max(0, int(sec))
    return f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"

def parse_args():
    p = argparse.ArgumentParser(description="Merge many ZIPs into one (streaming, progress, ETA).")
    p.add_argument("input_dir", help="Folder containing *.zip")
    p.add_argument("output_basename", help="Output base name (without .zip)")
    p.add_argument("--out-dir", default=None, help="Output directory (default: <input>_out)")
    p.add_argument("--filter", default="*.zip", help="Glob filter for input zip files")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--store", action="store_true", help="No compression")
    g.add_argument("--deflate", action="store_true", help="Deflate compression (default)")
    p.add_argument("--chunk-mb", type=int, default=4, help="I/O block size MB (default 4)")
    p.add_argument("--prefix-by-zip", action="store_true", help="Put entries under <zipname>/... (default: keep root)")
    p.add_argument("--split-size", default=None, help="Split result: e.g. 1900m, 2g (raw by default)")
    p.add_argument("--split-mode", default="zip", choices=["zip","raw"], help="zip -> 'zip -s', raw -> split file (default zip)")
    p.add_argument("--rm-after-split", action="store_true", help="Remove the big zip after splitting")
    return p.parse_args()

def parse_size(s: str) -> int:
    s = s.strip().lower()
    if not s:
        return 0
    num = "".join(ch for ch in s if ch.isdigit())
    suf = s[len(num):].strip()
    if not num:
        raise ValueError("Invalid size")
    n = int(num)
    mul = 1
    if suf in ("k","kb"):
        mul = 1024
    elif suf in ("m","mb"):
        mul = 1024*1024
    elif suf in ("g","gb"):
        mul = 1024*1024*1024
    elif suf in ("t","tb"):
        mul = 1024*1024*1024*1024
    return n*mul

def list_zips(input_dir, pattern):
    names = [f for f in os.listdir(input_dir) if f.lower().endswith(".zip") and fnmatch.fnmatch(f, pattern)]
    names.sort()
    return names

def zip_compressed_size(path):
    total = 0
    with zipfile.ZipFile(path, 'r', allowZip64=True) as z:
        for it in z.infolist():
            if it.is_dir(): continue
            total += max(0, getattr(it, "compress_size", 0))
    return total

def zip_compressed_total(paths):
    total = 0
    for p in paths:
        with zipfile.ZipFile(p, 'r', allowZip64=True) as z:
            for it in z.infolist():
                if not it.is_dir():
                    total += max(0, getattr(it, "compress_size", 0))
    return total

def zip_uncompressed_size(path):
    total = 0
    with zipfile.ZipFile(path, 'r', allowZip64=True) as z:
        for it in z.infolist():
            if it.is_dir(): continue
            total += max(0, getattr(it, "file_size", 0))
    return total

def raw_split(path: str, part_size_str: str, rm_after: bool):
    part_size = parse_size(part_size_str)
    if part_size <= 0:
        raise ValueError("split size must > 0")
    total = os.path.getsize(path)
    if total == 0: return
    prefix = path + ".part-"
    buf = bytearray(4*1024*1024)
    written = 0
    idx = 0
    with open(path, "rb") as f:
        while written < total:
            part_name = f"{prefix}{idx:03d}"
            with open(part_name, "wb") as out:
                copied = 0
                to_copy = min(part_size, total - written)
                while copied < to_copy:
                    n = f.readinto(buf)
                    if n <= 0: break
                    if copied + n > to_copy:
                        n = to_copy - copied
                    out.write(buf[:n])
                    copied += n
                    written += n
            print(f"Split part {part_name} ({human(copied)})")
            idx += 1
    if rm_after:
        os.remove(path)
        print(f"Removed original: {path}")
    print(f"Done raw split. To join:\n  cat {prefix}* > {os.path.basename(path)}")

def main():
    args = parse_args()
    input_dir = args.input_dir
    out_dir = args.out_dir or (input_dir.rstrip(os.sep) + "_output")
    os.makedirs(out_dir, exist_ok=True)
    output_file = os.path.join(out_dir, args.output_basename + ".zip")

    comp = zipfile.ZIP_STORED if args.store else zipfile.ZIP_DEFLATED
    chunk = max(1, args.chunk_mb) * 1024 * 1024

    names = list_zips(input_dir, args.filter)
    if not names:
        print(f"No zip matched '{args.filter}' in {input_dir}", file=sys.stderr)
        sys.exit(4)

    totals = [0]*len(names)
    overall_total = 0
    overall_compressed = 0
    for i, n in enumerate(names):
        p = os.path.join(input_dir, n)
        try:
            t = zip_uncompressed_size(p)
            c = zip_compressed_size(p)
        except zipfile.BadZipFile:
            print(f"WARNING: skip BadZipFile: {p}", file=sys.stderr)
            t, c = 0, 0
        totals[i] = t
        overall_total += t
        overall_compressed += c

    # ---- Disk space pre-check ----
    free = shutil.disk_usage(out_dir).free
    if args.store:
        need = int(overall_total * 1.05)  # ~5% central directory/overhead
        reason = "store (no compression)"
    else:
        # conservative: output should be near compressed size, but cap by uncompressed
        need = int(min(overall_total, overall_compressed * 1.25) * 1.10)
        reason = "deflate (recompression)"
    if free < need:
        print(f"ERROR: Not enough free space in {out_dir}. Need ~{need/1024/1024/1024:.1f} GB (mode={reason}), free {free/1024/1024/1024:.1f} GB.", file=sys.stderr)
        sys.exit(8)


    seen = {}
    overall_done = 0
    last_overall_pct = -1
    start = time.time()

    def map_target(zname, inner):
        inner = inner.lstrip("/\\")
        if args.prefix_by_zip:
            base = f"{os.path.splitext(zname)[0]}/{inner}"
        else:
            base = inner
        target = base
        cnt = seen.get(base, 0)
        if cnt > 0:
            root, ext = os.path.splitext(base)
            target = f"{root}__dup{cnt}{ext}"
        seen[base] = cnt + 1
        return target

    def print_line(msg):
        sys.stdout.write(msg + "\r")
        sys.stdout.flush()

    with zipfile.ZipFile(output_file, 'w', compression=comp, allowZip64=True) as zout:
        for idx, zname in enumerate(names, 1):
            zpath = os.path.join(input_dir, zname)
            try:
                with zipfile.ZipFile(zpath, 'r', allowZip64=True) as zin:
                    total_zip = totals[idx-1]
                    done_zip = 0
                    last_zip_pct = -1

                    for item in zin.infolist():
                        if item.is_dir(): continue
                        fn = item.filename
                        if fn.startswith("__MACOSX/") or fn.endswith(".DS_Store"): continue
                        new_name = map_target(zname, fn)
                        with zin.open(item, 'r') as src, zout.open(new_name, 'w') as dst:
                            while True:
                                buf = src.read(chunk)
                                if not buf: break
                                dst.write(buf)
                                blen = len(buf)
                                done_zip += blen
                                overall_done += blen
                                zip_pct = 100 if total_zip == 0 else int(done_zip*100//total_zip)
                                overall_pct = 100 if overall_total == 0 else int(overall_done*100//overall_total)
                                elapsed = time.time() - start
                                eta_str = "--:--:--"
                                if elapsed > 0 and overall_done > 0 and overall_total > overall_done:
                                    speed = overall_done/elapsed
                                    if speed > 0:
                                        eta = (overall_total-overall_done)/speed
                                        eta_str = hms(eta)
                                if zip_pct != last_zip_pct or overall_pct != last_overall_pct:
                                    last_zip_pct = zip_pct
                                    last_overall_pct = overall_pct
                                    print_line(f"[{idx}/{len(names)}] {zname}: {zip_pct:3d}% ({human(done_zip)}/{human(total_zip)})"
                                               f"  |  Overall: {overall_pct:3d}% ({human(overall_done)}/{human(overall_total)})"
                                               f"  |  Elapsed {hms(elapsed)}  ETA {eta_str}")
                    # finalize line
                    elapsed = time.time() - start
                    overall_pct = 100 if overall_total == 0 else int(overall_done*100//overall_total)
                    eta_str = "--:--:--"
                    if overall_done < overall_total and elapsed > 0 and overall_done > 0:
                        speed = overall_done/elapsed
                        if speed > 0:
                            eta = (overall_total-overall_done)/speed
                            eta_str = hms(eta)
                    print(f"[{idx}/{len(names)}] {zname}: 100% ({human(total_zip)}/{human(total_zip)})"
                          f"  |  Overall: {overall_pct:3d}% ({human(overall_done)}/{human(overall_total)})"
                          f"  |  Elapsed {hms(elapsed)}  ETA {eta_str}")
            except zipfile.BadZipFile:
                print(f"WARNING: skip BadZipFile: {zpath}", file=sys.stderr)

    print(f"Hoàn tất! Tạo: {output_file}")
    print(f"Total time: {hms(time.time()-start)}")

    # Optional split
    if args.split_size:
        if args.split_mode == "raw":
            raw_split(output_file, args.split_size, args.rm_after_split)
        else:
            # zip-split via external 'zip' CLI
            if shutil.which("zip") is None:
                print("ERROR: 'zip' CLI not found for --split-mode zip", file=sys.stderr)
                sys.exit(5)
            split_base = os.path.join(out_dir, args.output_basename + "-split.zip")
            print(f"Splitting via 'zip -s {args.split_size}' -> {split_base} (+ .z01, .z02, ...)")
            subprocess.run(["zip","-s", args.split_size, output_file, "--out", split_base], check=True)
            print(f"Done split: {split_base}")
            if args.rm_after_split:
                os.remove(output_file)
                print(f"Removed original: {output_file}")

if __name__ == "__main__":
    main()
