extern crate libc;
use clap::Parser;
use glob::Pattern;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use zip::read::ZipArchive;
use zip::write::FileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;
use libc::statvfs;

#[derive(Parser, Debug)]
#[command(name="mergezip_rs", version, about="Merge many ZIPs (streaming, progress, ETA)")]
struct Opts {
    /// Input directory containing *.zip
    #[arg(long, default_value="abcxyz")]
    input: String,
    /// Output directory (default: <input>_out)
    #[arg(long, default_value="")]
    outdir: String,
    /// Output base name (without .zip)
    #[arg(long, default_value="merged")]
    out: String,
    /// Filter glob for input files
    #[arg(long, default_value="*.zip")]
    filter: String,
    /// Store (no compression)
    #[arg(long, default_value_t=false)]
    store: bool,
    /// I/O block size MB
    #[arg(long, default_value_t=4)]
    chunk: usize,
    /// Prefix entries by zip filename (old behavior)
    #[arg(long, default_value_t=false)]
    prefix_by_zip: bool,
    /// Raw split size (e.g., 1900m, 2g). If empty, no split.
    #[arg(long, default_value="")]
    split: String,
    /// Remove big zip after split
    #[arg(long, default_value_t=false)]
    rm_after_split: bool,
}

fn human(mut n: u64) -> String {
    let units = ["B","KB","MB","GB","TB"];
    let mut f = n as f64;
    let mut i = 0usize;
    while f >= 1024.0 && i < units.len()-1 {
        f /= 1024.0; i += 1;
    }
    format!("{:.1} {}", f, units[i])
}
fn hms(mut sec: i64) -> String {
    if sec < 0 { sec = 0; }
    let h = sec / 3600;
    let m = (sec % 3600) / 60;
    let s = sec % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}
fn parse_size(s: &str) -> io::Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() { return Ok(0); }
    let mut num = String::new();
    let mut suf = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() { num.push(c); } else { suf.push(c); }
    }
    if num.is_empty() { return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid size")); }
    let n: u64 = num.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "parse size"))?;
    let mul = match suf.as_str() {
        "k" | "kb" => 1024u64,
        "m" | "mb" => 1024u64*1024,
        "g" | "gb" => 1024u64*1024*1024,
        "t" | "tb" => 1024u64*1024*1024*1024,
        _ => 1u64,
    };
    Ok(n*mul)
}
fn list_zip_files(dir: &Path, pattern: &str) -> io::Result<Vec<String>> {
    let pat = Pattern::new(pattern).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid glob"))?;
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.to_lowercase().ends_with(".zip") && pat.matches(&name) {
                out.push(name);
            }
        }
    }
    out.sort();
    Ok(out)
}
fn should_skip(p: &str) -> bool {
    p.is_empty() || p.starts_with("__MACOSX/") || p.ends_with(".DS_Store")
}
fn map_target(prefix_by_zip: bool, zip_name: &str, inner: &str, dedup: &mut HashMap<String, usize>) -> String {
    let mut base = inner.trim_start_matches(['/', '\\']);
    let base_owned;
    if prefix_by_zip {
        let prefix = Path::new(zip_name).file_stem().unwrap_or_default().to_string_lossy();
        base_owned = format!("{}/{}", prefix, base);
        base = &base_owned;
    }
    let key = base.replace("\\", "/");
    let count = *dedup.get(&key).unwrap_or(&0);
    dedup.insert(key.clone(), count+1);
    if count > 0 {
        if let Some(dot) = key.rfind('.') {
            format!("{}__dup{}{}", &key[..dot], count, &key[dot..])
        } else {
            format!("{}__dup{}", key, count)
        }
    } else {
        key
    }
}
fn raw_split(path: &Path, size_str: &str, rm_after: bool) -> io::Result<()> {
    let part = parse_size(size_str)?;
    if part == 0 { return Err(io::Error::new(io::ErrorKind::InvalidInput, "split size must > 0")); }
    let mut f = File::open(path)?;
    let meta = f.metadata()?;
    let total = meta.len();
    if total == 0 { return Ok(()); }
    let mut buf = vec![0u8; 4*1024*1024];
    let mut written: u64 = 0;
    let mut idx = 0u32;
    while written < total {
        let part_name = format!("{}.part-{:03}", path.display(), idx);
        let mut out = File::create(&part_name)?;
        let mut copied: u64 = 0;
        while copied < part && written < total {
            let to_read = std::cmp::min(buf.len() as u64, std::cmp::min(part - copied, total - written));
            let n = f.read(&mut buf[..to_read as usize])?;
            if n == 0 { break; }
            out.write_all(&buf[..n])?;
            copied += n as u64;
            written += n as u64;
        }
        drop(out);
        println!("Split part {} ({})", part_name, human(copied));
        idx += 1;
    }
    if rm_after {
        fs::remove_file(path)?;
        println!("Removed original: {}", path.display());
    }
    println!("Done raw split. To join:\n  cat {}.part-* > {}", path.display(), path.file_name().unwrap().to_string_lossy());
    Ok(())
}

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    let input_dir = PathBuf::from(&opts.input);
    if !input_dir.is_dir() { return Err(io::Error::new(io::ErrorKind::NotFound, "input dir not found")); }
    let out_dir = if opts.outdir.is_empty() {
        PathBuf::from(format!("{}_output", input_dir.to_string_lossy()))
    } else {
        PathBuf::from(&opts.outdir)
    };
    fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join(format!("{}.zip", opts.out));

    let names = list_zip_files(&input_dir, &opts.filter)?;
    if names.is_empty() {
        eprintln!("No zip matched '{}' in {}", opts.filter, input_dir.display());
        std::process::exit(4);
    }

    // totals
    let mut overall_total: u64 = 0;
    let mut totals: Vec<u64> = Vec::with_capacity(names.len());
    for name in &names {
        let file = File::open(input_dir.join(name))?;
        let mut zr = ZipArchive::new(file).unwrap();
        let mut total = 0u64;
        for i in 0..zr.len() {
            let f = zr.by_index(i).unwrap();
            if f.is_dir() { continue; }
            total += f.size();
        }
        totals.push(total);
        overall_total += total;
    }

    
    // ---- Disk space pre-check ----
    // compute compressed total
    let mut overall_compressed: u64 = 0;
    for name in &names {
        let file = File::open(input_dir.join(name))?;
        let mut zr = ZipArchive::new(file).unwrap();
        for i in 0..zr.len() {
            let f = zr.by_index(i).unwrap();
            if f.is_dir() { continue; }
            overall_compressed += f.compressed_size();
        }
    }
    // statvfs
    let mut free_bytes: u64 = 0;
    unsafe {
        use std::ffi::CString;
        let cpath = CString::new(out_dir.to_string_lossy().to_string()).unwrap();
        let mut s = std::mem::zeroed::<libc::statvfs>();
        if statvfs(cpath.as_ptr(), &mut s) == 0 {
            free_bytes = (s.f_bavail as u64) * (s.f_frsize as u64);
        }
    }
    let need: u64;
    let reason: &str;
    if opts.store {
        need = (overall_total as f64 * 1.05) as u64;
        reason = "store (no compression)";
    } else {
        let mut candidate = (overall_compressed as f64 * 1.25) as u64;
        if candidate > overall_total { candidate = overall_total; }
        need = (candidate as f64 * 1.10) as u64;
        reason = "deflate (recompression)";
    }
    if free_bytes > 0 && free_bytes < need {
        eprintln!("ERROR: Not enough free space in {}. Need ~{:.1} GB (mode={}), free {:.1} GB.",
            out_dir.display(),
            need as f64 / 1024.0 / 1024.0 / 1024.0,
            reason,
            free_bytes as f64 / 1024.0 / 1024.0 / 1024.0
        );
        std::process::exit(8);
    }

    let of = File::create(&out_path)?;
    let mut zw = ZipWriter::new(BufWriter::new(of));
    let options = if opts.store { FileOptions::default().compression_method(CompressionMethod::Stored) }
                  else { FileOptions::default().compression_method(CompressionMethod::Deflated) };

    let start = Instant::now();
    let mut overall_done: u64 = 0;
    let mut dedup: HashMap<String, usize> = HashMap::new();
    let mut buf = vec![0u8; std::cmp::max(1, opts.chunk) * 1024 * 1024];

    for (idx, name) in names.iter().enumerate() {
        let file = File::open(input_dir.join(name))?;
        let mut zr = ZipArchive::new(file).unwrap();
        let total_zip = totals[idx];
        let mut done_zip: u64 = 0;
        let mut last_zip_pct = -1i32;
        let mut last_all_pct = -1i32;
        let prefix = format!("[{}/{}] {}", idx+1, names.len(), name);

        for i in 0..zr.len() {
            let mut f = zr.by_index(i).unwrap();
            if f.is_dir() { continue; }
            let inner = f.name().to_string();
            if should_skip(&inner) { continue; }
            let target = map_target(opts.prefix_by_zip, name, &inner, &mut dedup);
            let mut opts2 = options.clone();
            if let Some(time) = f.last_modified().to_time() {
                let tm = time.to_timespec();
                opts2 = opts2.last_modified_time(zip::DateTime::from_time(time).unwrap_or(zip::DateTime::default()));
            }
            zw.start_file(target, opts2)?;
            loop {
                let n = f.read(&mut buf)?;
                if n == 0 { break; }
                zw.write_all(&buf[..n])?;
                done_zip += n as u64;
                overall_done += n as u64;
                let zp = if total_zip == 0 { 100 } else { (done_zip * 100 / total_zip) as i32 };
                let ap = if overall_total == 0 { 100 } else { (overall_done * 100 / overall_total) as i32 };
                if zp != last_zip_pct || ap != last_all_pct {
                    last_zip_pct = zp; last_all_pct = ap;
                    let elapsed = start.elapsed();
                    let eta = if overall_done > 0 && overall_done < overall_total {
                        let speed = overall_done as f64 / elapsed.as_secs_f64();
                        if speed > 0.0 {
                            let remain = (overall_total - overall_done) as f64 / speed;
                            Duration::from_secs(remain as u64)
                        } else { Duration::ZERO }
                    } else { Duration::ZERO };
                    print!("\r{}: {:3}% ({}/{})  |  Overall: {:3}% ({}/{})  |  Elapsed {}  ETA {}",
                        prefix, zp, human(done_zip), human(total_zip),
                        ap, human(overall_done), human(overall_total),
                        hms(elapsed.as_secs() as i64), hms(eta.as_secs() as i64));
                    io::stdout().flush().ok();
                }
            }
        }
        // finalize line
        let elapsed = start.elapsed();
        let ap = if overall_total == 0 { 100 } else { (overall_done * 100 / overall_total) as i32 };
        println!("\r{}: 100% ({}/{})  |  Overall: {:3}% ({}/{})  |  Elapsed {}  ETA --:--:--",
            prefix, human(total_zip), human(total_zip),
            ap, human(overall_done), human(overall_total),
            hms(elapsed.as_secs() as i64));
    }

    zw.finish()?;
    let elapsed = start.elapsed();
    println!("Hoàn tất! Tạo: {}", out_path.display());
    println!("Total time: {}", hms(elapsed.as_secs() as i64));

    if !opts.split.is_empty() {
        raw_split(&out_path, &opts.split, opts.rm_after_split)?;
    }
    Ok(())
}
