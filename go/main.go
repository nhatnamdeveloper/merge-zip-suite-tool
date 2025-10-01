package main

import (
	"archive/zip"
	"bufio"
	"compress/flate"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"syscall"
	"runtime"
)

type options struct {
	inputDir      string
	outDir        string
	outBase       string
	filterGlob    string
	store         bool
	deflateLevel  int
	chunkMB       int
	prefixByZip   bool
	splitSize     string
	splitMode     string
	rmAfterSplit  bool
}

func parseFlags() (options, error) {
	var opt options
	flag.StringVar(&opt.inputDir, "input", "abcxyz", "Thư mục chứa .zip nguồn")
	flag.StringVar(&opt.outDir, "outdir", "", "Thư mục output (mặc định: <input>_out)")
	flag.StringVar(&opt.outBase, "out", "merged", "Tên file đầu ra (không kèm .zip)")
	flag.StringVar(&opt.filterGlob, "filter", "*.zip", "Glob lọc (vd: 'part-*.zip')")
	flag.BoolVar(&opt.store, "store", false, "Ghi không nén (nhanh hơn, file to hơn)")
	flag.IntVar(&opt.deflateLevel, "level", flate.DefaultCompression, "Mức nén Deflate (-2..9)")
	flag.IntVar(&opt.chunkMB, "chunk", 4, "Block I/O (MB)")
	flag.BoolVar(&opt.prefixByZip, "prefix-by-zip", false, "Lồng theo tên zip gốc (mặc định: giữ root)")
	flag.StringVar(&opt.splitSize, "split", "", "Chia nhỏ file đầu ra (raw split), vd: 1900m, 2g")
	flag.StringVar(&opt.splitMode, "splitmode", "raw", "Chế độ split: raw (mặc định)")
	flag.BoolVar(&opt.rmAfterSplit, "rm-after-split", false, "Xoá file .zip lớn sau khi split")
	flag.Parse()

	if opt.outBase == "" {
		return opt, errors.New("out basename rỗng")
	}
	if opt.chunkMB <= 0 {
		opt.chunkMB = 4
	}
	if opt.outDir == "" {
		opt.outDir = strings.TrimRight(opt.inputDir, string(os.PathSeparator)) + "_output"
	}
	return opt, nil
}

func listZipFiles(dir, glob string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil { return nil, err }
	var out []string
	for _, e := range ents {
		if e.IsDir() { continue }
		name := e.Name()
		match, err := filepath.Match(glob, name)
		if err != nil { return nil, err }
		if match && strings.HasSuffix(strings.ToLower(name), ".zip") {
			out = append(out, name)
		}
	}
	// simple sort
	for i := 0; i < len(out); i++ {
		for j := i+1; j < len(out); j++ {
			if out[j] < out[i] { out[i], out[j] = out[j], out[i] }
		}
	}
	return out, nil
}

func humanBytes(n uint64) string {
	const k = 1024.0
	f := float64(n)
	unit := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	for f >= k && i < len(unit)-1 { f /= k; i++ }
	return fmt.Sprintf("%.1f %s", f, unit[i])
}

func fmtHMS(d time.Duration) string {
	if d < 0 { d = 0 }
	s := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d:%02d", s/3600, (s%3600)/60, s%60)
}

func sumUncompressed(zr *zip.ReadCloser) uint64 {
	var total uint64
	for _, f := range zr.File {
		if f.FileInfo().IsDir() { continue }
		total += f.UncompressedSize64
	}
	return total
}

func shouldSkipPath(p string) bool {
	if p == "" { return true }
	if strings.HasPrefix(p, "__MACOSX/") { return true }
	if strings.HasSuffix(p, ".DS_Store") { return true }
	return false
}

func mapTargetName(prefixByZip bool, zipName, inner string, dedup map[string]int) string {
	inner = strings.TrimLeft(inner, "/\\")
	var base string
	if prefixByZip {
		prefix := strings.TrimSuffix(zipName, filepath.Ext(zipName))
		base = filepath.ToSlash(filepath.Join(prefix, inner))
	} else {
		base = filepath.ToSlash(inner) // giữ root
	}
	target := base
	if c, ok := dedup[base]; ok {
		root, ext := base, ""
		if dot := strings.LastIndex(base, "."); dot >= 0 { root, ext = base[:dot], base[dot:] }
		target = fmt.Sprintf("%s__dup%d%s", root, c, ext)
		dedup[base] = c + 1
	} else {
		dedup[base] = 1
	}
	return target
}

func printZipProgress(prefix string, done, total, overallDone, overallTotal uint64, start time.Time, lastZipPct, lastAllPct *int) {
	zp := 100
	if total > 0 { zp = int((done * 100) / total) }
	ap := 100
	if overallTotal > 0 { ap = int((overallDone * 100) / overallTotal) }
	if zp != *lastZipPct || ap != *lastAllPct {
		*lastZipPct = zp
		*lastAllPct = ap
		elapsed := time.Since(start)
		etaStr := "--:--:--"
		if overallDone > 0 && overallDone < overallTotal && elapsed > 0 {
			speed := float64(overallDone) / elapsed.Seconds()
			if speed > 0 {
				remain := float64(overallTotal - overallDone)
				etaStr = fmtHMS(time.Duration(remain/speed) * time.Second)
			}
		}
		fmt.Printf("\r%s: %3d%% (%s/%s)  |  Overall: %3d%% (%s/%s)  |  Elapsed %s  ETA %s",
			prefix,
			zp, humanBytes(done), humanBytes(total),
			ap, humanBytes(overallDone), humanBytes(overallTotal),
			fmtHMS(elapsed), etaStr,
		)
	}
}

func registerDeflater(z *zip.Writer, level int) {
	z.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		if level == -2 { return flate.NewWriter(w, flate.HuffmanOnly) }
		return flate.NewWriter(w, level)
	})
}

var splitSizeRe = regexp.MustCompile(`(?i)^\s*([0-9]+)\s*([kmgt]?)\s*$`)

func parseSize(s string) (int64, error) {
	m := splitSizeRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil { return 0, fmt.Errorf("kích thước không hợp lệ: %q", s) }
	num, _ := strconv.ParseInt(m[1], 10, 64)
	mul := int64(1)
	switch strings.ToLower(m[2]) {
	case "k": mul = 1024
	case "m": mul = 1024 * 1024
	case "g": mul = 1024 * 1024 * 1024
	case "t": mul = 1024 * 1024 * 1024 * 1024
	}
	return num * mul, nil
}

func rawSplit(path, partSizeStr string, rmAfter bool) error {
	partSize, err := parseSize(partSizeStr)
	if err != nil { return err }
	if partSize <= 0 { return fmt.Errorf("split size phải > 0") }

	in, err := os.Open(path)
	if err != nil { return err }
	defer in.Close()

	info, err := in.Stat()
	if err != nil { return err }
	total := info.Size()
	if total == 0 { return nil }

	prefix := path + ".part-"
	buf := make([]byte, 4*1024*1024)
	var partIdx int
	var written int64

	for {
		partName := fmt.Sprintf("%s%03d", prefix, partIdx)
		out, err := os.Create(partName)
		if err != nil { return err }

		var copied int64
		for copied < partSize {
			toRead := int64(len(buf))
			if remain := partSize - copied; toRead > remain { toRead = remain }
			n, rErr := in.Read(buf[:toRead])
			if n > 0 {
				if _, wErr := out.Write(buf[:n]); wErr != nil { _ = out.Close(); return wErr }
				copied += int64(n)
				written += int64(n)
			}
			if rErr != nil {
				if rErr == io.EOF { break }
				_ = out.Close(); return rErr
			}
		}
		_ = out.Close()
		fmt.Printf("Split part %s (%s)\n", partName, humanBytes(uint64(copied)))
		if written >= total { break }
		partIdx++
	}

	if rmAfter {
		if err := os.Remove(path); err != nil { return err }
		fmt.Printf("Removed original: %s\n", path)
	}
	fmt.Printf("Done raw split. To join:\n  cat %s* > %s\n", prefix, filepath.Base(path))
	return nil
}

func mergeZIP(opt options) (string, error) {
	if err := os.MkdirAll(opt.outDir, 0o755); err != nil { return "", err }
	outPath := filepath.Join(opt.outDir, opt.outBase+".zip")

	names, err := listZipFiles(opt.inputDir, opt.filterGlob)
	if err != nil { return "", err }
	if len(names) == 0 { return "", fmt.Errorf("không tìm thấy .zip khớp '%s' trong %s", opt.filterGlob, opt.inputDir) }

	var overallTotal uint64
	zipTotals := make([]uint64, len(names))
	for i, name := range names {
		zr, err := zip.OpenReader(filepath.Join(opt.inputDir, name))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: bỏ qua (không mở được): %s (%v)\n", name, err)
			continue
		}
		zipTotals[i] = sumUncompressed(zr)
		overallTotal += zipTotals[i]
		_ = zr.Close()
	}

	
	// ---- Disk space pre-check ----
	var overallCompressed uint64 = 0
	for _, name := range names {
		p := filepath.Join(opt.inputDir, name)
		zr, err := zip.OpenReader(p)
		if err != nil { continue }
		for _, f := range zr.File {
			if f.FileInfo().IsDir() { continue }
			overallCompressed += f.CompressedSize64
		}
		_ = zr.Close()
	}
	var freeBytes uint64 = 0
	if runtime.GOOS != "windows" {
		var fs syscall.Statfs_t
		if err := syscall.Statfs(opt.outDir, &fs); err == nil {
			freeBytes = fs.Bavail * uint64(fs.Bsize)
		}
	}
	var need uint64
	reason := ""
	if opt.store {
		need = uint64(float64(overallTotal) * 1.05)
		reason = "store (no compression)"
	} else {
		candidate := uint64(float64(overallCompressed) * 1.25)
		if candidate > overallTotal { candidate = overallTotal }
		need = uint64(float64(candidate) * 1.10)
		reason = "deflate (recompression)"
	}
	if freeBytes > 0 && freeBytes < need {
		return "", fmt.Errorf("không đủ dung lượng trống ở %s: cần ~%.1f GB (mode=%s), còn %.1f GB",
			opt.outDir, float64(need)/1024/1024/1024, reason, float64(freeBytes)/1024/1024/1024)
	}

	outFile, err := os.Create(outPath)
	if err != nil { return "", err }
	defer outFile.Close()

	zw := zip.NewWriter(outFile)
	if !opt.store { registerDeflater(zw, opt.deflateLevel) }
	defer zw.Close()

	start := time.Now()
	var overallDone uint64
	dedup := map[string]int{}
	buf := make([]byte, opt.chunkMB*1024*1024)
	if len(buf) == 0 { buf = make([]byte, 4*1024*1024) }

	for idx, name := range names {
		zr, err := zip.OpenReader(filepath.Join(opt.inputDir, name))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: bỏ qua (không mở được): %s (%v)\n", name, err)
			continue
		}
		totalZip := zipTotals[idx]
		var doneZip uint64
		lastZipPct, lastAllPct := -1, -1
		prefix := fmt.Sprintf("[%d/%d] %s", idx+1, len(names), name)

		for _, f := range zr.File {
			if f.FileInfo().IsDir() { continue }
			if shouldSkipPath(f.Name) { continue }
			target := mapTargetName(opt.prefixByZip, name, f.Name, dedup)

			hdr := &zip.FileHeader{Name: filepath.ToSlash(target), Method: zip.Store}
			if !opt.store { hdr.Method = zip.Deflate }
			if !f.Modified.IsZero() { hdr.SetModTime(f.Modified) } else { hdr.SetModTime(time.Now()) }
			hdr.UncompressedSize64 = f.UncompressedSize64

			w, err := zw.CreateHeader(hdr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nWARNING: không thể tạo entry '%s': %v\n", hdr.Name, err)
				continue
			}
			rc, err := f.Open()
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nWARNING: không thể đọc '%s' trong %s: %v\n", f.Name, name, err)
				continue
			}

			bw := bufio.NewWriter(w)
			for {
				n, rErr := rc.Read(buf)
				if n > 0 {
					if _, wErr := bw.Write(buf[:n]); wErr != nil {
						_ = rc.Close(); _ = bw.Flush(); _ = zr.Close()
						return "", wErr
					}
					doneZip += uint64(n)
					overallDone += uint64(n)
					printZipProgress(prefix, doneZip, totalZip, overallDone, overallTotal, start, &lastZipPct, &lastAllPct)
				}
				if rErr != nil {
					if rErr == io.EOF { break }
					fmt.Fprintf(os.Stderr, "\nWARNING: lỗi đọc entry '%s' trong %s: %v\n", f.Name, name, rErr)
					break
				}
			}
			_ = rc.Close()
			_ = bw.Flush()
		}
		printZipProgress(prefix, totalZip, totalZip, overallDone, overallTotal, start, &lastZipPct, &lastAllPct)
		fmt.Print("\n")
		_ = zr.Close()
	}

	if err := zw.Close(); err != nil { return "", err }
	if err := outFile.Close(); err != nil { return "", err }
	fmt.Printf("Hoàn tất! Tạo: %s\n", outPath)
	fmt.Printf("Total time: %s\n", fmtHMS(time.Since(start)))
	return outPath, nil
}

func main() {
	opt, err := parseFlags()
	if err != nil { fmt.Fprintln(os.Stderr, "ERROR:", err); os.Exit(2) }

	outPath, err := mergeZIP(opt)
	if err != nil { fmt.Fprintln(os.Stderr, "ERROR:", err); os.Exit(1) }

	if opt.splitSize != "" {
		if strings.ToLower(opt.splitMode) != "raw" {
			fmt.Println("NOTE: zip-split (.z01, .z02, ...) chưa hiện thực trong Go; dùng `zip -s` bên ngoài.")
		}
		if err := rawSplit(outPath, opt.splitSize, opt.rmAfterSplit); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR split:", err); os.Exit(3)
		}
	}
}
