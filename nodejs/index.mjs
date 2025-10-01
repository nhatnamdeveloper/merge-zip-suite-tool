import fs from "fs";
import path from "path";
import {fileURLToPath} from "url";
import minimatch from "minimatch";
import yauzl from "yauzl";
import yazl from "yazl";
import { execSync } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function human(n) {
  const units = ["B","KB","MB","GB","TB"];
  let f = Number(n);
  for (let i=0;i<units.length;i++) {
    if (f < 1024 || i === units.length-1) return `${f.toFixed(1)} ${units[i]}`;
    f /= 1024;
  }
}
function hms(sec) {
  sec = Math.max(0, Math.floor(sec));
  const h = Math.floor(sec/3600);
  const m = Math.floor((sec%3600)/60);
  const s = sec%60;
  return `${h.toString().padStart(2,"0")}:${m.toString().padStart(2,"0")}:${s.toString().padStart(2,"0")}`;
}
function parseArgs() {
  const args = process.argv.slice(2);
  const opt = { input: "abcxyz", outdir: "", out: "merged", filter: "*.zip", store: false, chunk: 4, prefixByZip: false, split: "", rmAfterSplit: false };
  for (let i=0;i<args.length;i++) {
    const a = args[i];
    if (a === "--input") opt.input = args[++i];
    else if (a === "--outdir") opt.outdir = args[++i];
    else if (a === "--out") opt.out = args[++i];
    else if (a === "--filter") opt.filter = args[++i];
    else if (a === "--store") opt.store = true;
    else if (a === "--chunk") opt.chunk = parseInt(args[++i]||"4",10);
    else if (a === "--prefix-by-zip") opt.prefixByZip = true;
    else if (a === "--split") opt.split = args[++i];
    else if (a === "--rm-after-split") opt.rmAfterSplit = true;
  }
  if (!opt.outdir) opt.outdir = path.join(opt.input.replace(/[\\\/]$/,"") + "_output");
  return opt;
}
function listZips(dir, glob) {
  return fs.readdirSync(dir)
    .filter(n => n.toLowerCase().endsWith(".zip") && minimatch(n, glob))
    .sort();
}
function openZip(file) {
  return new Promise((resolve, reject) => {
    yauzl.open(file, { lazyEntries: true, autoClose: true }, (err, zip) => {
      if (err || !zip) return reject(err || new Error("open failed"));
      resolve(zip);
    });
  });
}
function getEntriesTotalUncompressed(zipFile) {
  return new Promise((resolve, reject) => {
    let total = 0;
    zipFile.readEntry();
    zipFile.on("entry", (entry) => {
      if (!/\/$/.test(entry.fileName)) {
        total += entry.uncompressedSize;
      }
      zipFile.readEntry();
    });
    zipFile.on("end", () => resolve(total));
    zipFile.on("error", reject);
  });
}
function openEntryStream(zipFile, entry) {
  return new Promise((resolve, reject) => {
    zipFile.openReadStream(entry, (err, stream) => {
      if (err) return reject(err);
      resolve(stream);
    });
  });
}
function parseSize(s) {
  if (!s) return 0;
  const m = String(s).trim().toLowerCase().match(/^(\d+)\s*([kmgt]?)b?$/);
  if (!m) throw new Error("invalid size: "+s);
  const n = parseInt(m[1],10);
  const suf = m[2];
  const mul = suf === "k" ? 1024 : suf === "m"? 1024**2 : suf === "g"? 1024**3 : suf === "t"? 1024**4 : 1;
  return n*mul;
}
async function rawSplit(p, sizeStr, rmAfter) {
  const partSize = parseSize(sizeStr);
  if (partSize <= 0) throw new Error("split size must > 0");
  const stat = fs.statSync(p);
  const total = stat.size;
  const fd = fs.openSync(p, "r");
  const buf = Buffer.alloc(4*1024*1024);
  let written = 0, idx = 0;
  try {
    while (written < total) {
      const part = `${p}.part-${String(idx).padStart(3,"0")}`;
      const out = fs.openSync(part, "w");
      let copied = 0;
      while (copied < partSize && written < total) {
        const toRead = Math.min(buf.length, partSize - copied, total - written);
        const { bytesRead } = fs.readSync(fd, buf, 0, toRead, written);
        if (bytesRead <= 0) break;
        fs.writeSync(out, buf, 0, bytesRead);
        copied += bytesRead; written += bytesRead;
      }
      fs.closeSync(out);
      console.log(`Split part ${part} (${human(copied)})`);
      idx++;
    }
  } finally {
    fs.closeSync(fd);
  }
  if (rmAfter) { fs.unlinkSync(p); console.log(`Removed original: ${p}`); }
  console.log(`Done raw split. To join:\n  cat ${p}.part-* > ${path.basename(p)}`);
}
async function main() {
  const opt = parseArgs();
  const inputDir = opt.input;
  const outdir = opt.outdir;
  const outPath = path.join(outdir, opt.out + ".zip");
  fs.mkdirSync(outdir, { recursive: true });

  const names = listZips(inputDir, opt.filter);
  if (names.length === 0) {
    console.error(`No zip matched '${opt.filter}' in ${inputDir}`);
    process.exit(4);
  }

  // First pass: totals
  let overallTotal = 0;
  const totals = [];
  for (const name of names) {
    const z = await openZip(path.join(inputDir, name));
    const t = await getEntriesTotalUncompressed(z).catch(() => 0);
    totals.push(t);
    overallTotal += t;
    z.close();
  }

  
  // ---- Disk space pre-check ----
  async function compressedTotal(paths) {
    let tot = 0;
    for (const p of paths) {
      const zc = await openZip(p);
      zc.readEntry();
      await new Promise((resolve) => {
        zc.on("entry", (e) => { if (!/\/$/.test(e.fileName)) tot += e.compressedSize || 0; zc.readEntry(); });
        zc.on("end", resolve); zc.on("error", resolve);
      });
      zc.close();
    }
    return tot;
  }
  function freeBytes(dir) {
    try {
      const out = execSync(`df -k ${JSON.stringify(dir)}`, {stdio: ["ignore","pipe","ignore"]}).toString();
      const parts = out.trim().split(/\r?\n/).pop().trim().split(/\s+/);
      const availK = parseInt(parts[3], 10);
      return isNaN(availK) ? 0 : availK*1024;
    } catch { return 0; }
  }
  const srcPaths = names.map(n => path.join(inputDir, n));
  const overallCompressed = await compressedTotal(srcPaths);
  const free = freeBytes(outdir);
  let need, reason;
  if (opt.store) {
    need = Math.floor(overallTotal * 1.05);
    reason = "store (no compression)";
  } else {
    let candidate = Math.floor(overallCompressed * 1.25);
    if (candidate > overallTotal) candidate = overallTotal;
    need = Math.floor(candidate * 1.10);
    reason = "deflate (recompression)";
  }
  if (free > 0 && free < need) {
    console.error(`ERROR: Not enough free space in ${outdir}. Need ~${(need/1024/1024/1024).toFixed(1)} GB (mode=${reason}), free ${(free/1024/1024/1024).toFixed(1)} GB.`);
    process.exit(8);
  }

  const zipWriter = new yazl.ZipFile();
  const outStream = fs.createWriteStream(outPath);
  zipWriter.outputStream.pipe(outStream);

  const seen = new Map();
  let overallDone = 0;
  let lastOverallPct = -1;
  const start = Date.now();

  function mapTarget(zipName, inner) {
    inner = inner.replace(/^([\/\\])+/,"");
    let base;
    if (opt.prefixByZip) {
      base = path.posix.join(zipName.replace(/\.zip$/i,""), inner);
    } else {
      base = inner.replace(/\\/g,"/");
    }
    const count = seen.get(base) || 0;
    seen.set(base, count+1);
    if (count > 0) {
      const i = base.lastIndexOf(".");
      const root = i>=0 ? base.slice(0,i) : base;
      const ext = i>=0 ? base.slice(i) : "";
      return `${root}__dup${count}${ext}`;
    }
    return base;
  }

  function printProgress(prefix, doneZip, totalZip) {
    const zp = totalZip === 0 ? 100 : Math.floor(doneZip*100/totalZip);
    const ap = overallTotal === 0 ? 100 : Math.floor(overallDone*100/overallTotal);
    const elapsed = (Date.now() - start)/1000;
    let etaStr = "--:--:--";
    if (overallDone>0 && overallDone<overallTotal && elapsed>0) {
      const speed = overallDone/elapsed;
      if (speed>0) {
        const eta = (overallTotal-overallDone)/speed;
        etaStr = hms(eta);
      }
    }
    if (ap !== lastOverallPct) {
      lastOverallPct = ap;
      process.stdout.write(`\r${prefix}: ${String(zp).padStart(3," ")}% (${human(doneZip)}/${human(totalZip)})  |  Overall: ${String(ap).padStart(3," ")}% (${human(overallDone)}/${human(overallTotal)})  |  Elapsed ${hms(elapsed)}  ETA ${etaStr}`);
    }
  }

  for (let idx=0; idx<names.length; idx++) {
    const name = names[idx];
    const p = path.join(inputDir, name);
    const totalZip = totals[idx];
    let doneZip = 0;
    let lastPrinted = -1;

    const z = await openZip(p);
    await new Promise((resolve, reject) => {
      z.readEntry();
      z.on("entry", async (entry) => {
        if (/\/$/.test(entry.fileName) || entry.fileName.startsWith("__MACOSX/") || entry.fileName.endsWith(".DS_Store")) {
          z.readEntry();
          return;
        }
        try {
          const rs = await openEntryStream(z, entry);
          const target = mapTarget(name, entry.fileName);
          // wrap to count bytes
          let pending = true;
          const counting = new (await import("stream")).Transform({
            transform(chunk, enc, cb) {
              doneZip += chunk.length;
              overallDone += chunk.length;
              const prefix = `[${idx+1}/${names.length}] ${name}`;
              const zp = totalZip === 0 ? 100 : Math.floor(doneZip*100/totalZip);
              if (zp !== lastPrinted) {
                lastPrinted = zp;
                // update both % lines
                printProgress(prefix, doneZip, totalZip);
              }
              cb(null, chunk);
            }
          });
          const options = { compress: !opt.store, mtime: new Date(entry.getLastModDate()) };
          zipWriter.addReadStream(rs.pipe(counting), target, options);
          rs.on("end", () => {
            z.readEntry();
          });
          rs.on("error", reject);
        } catch (e) {
          reject(e);
        }
      });
      z.on("end", resolve);
      z.on("error", reject);
    });
    // finalize line for this zip
    const elapsed = (Date.now()-start)/1000;
    const ap = overallTotal === 0 ? 100 : Math.floor(overallDone*100/overallTotal);
    process.stdout.write(`\r[${idx+1}/${names.length}] ${name}: 100% (${human(totalZip)}/${human(totalZip)})  |  Overall: ${String(ap).padStart(3," ")}% (${human(overallDone)}/${human(overallTotal)})  |  Elapsed ${hms(elapsed)}  ETA --:--:--\n`);
    z.close();
  }

  zipWriter.end();
  await new Promise((res, rej) => {
    outStream.on("close", res);
    outStream.on("error", rej);
  });
  const totalElapsed = (Date.now()-start)/1000;
  console.log(`Hoàn tất! Tạo: ${outPath}`);
  console.log(`Total time: ${hms(totalElapsed)}`);

  if (opt.split) {
    await rawSplit(outPath, opt.split, opt.rmAfterSplit);
  }
}

main().catch(err => {
  console.error("ERROR:", err);
  process.exit(1);
});
