package simfs_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// onDisk runs fn on one machine's simulated disk; powerLoss crashes the
// machine (power loss) and restarts it.
func onDisk(t *testing.T, seed uint64, opts simfs.Options, fn func(fsys dst.FS, disk *simworld.Machine, powerLoss func(), check func(bool, string, ...any))) {
	t.Helper()
	sim := simworld.New(simworld.Options{Seed: seed})
	disk := sim.AddMachine("disk", simworld.MachineOptions{Disk: opts})
	var failure error
	check := func(cond bool, format string, args ...any) {
		if !cond && failure == nil {
			failure = fmt.Errorf(format, args...)
		}
	}
	powerLoss := func() { disk.Crash(); sim.S.Restart(disk.M) }
	res := sim.Run(func(*dst.World) { fn(disk.World.FS(), disk, powerLoss, check) })
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if failure != nil {
		t.Fatalf("seed %d: %v", seed, failure)
	}
}

func readAll(fsys dst.FS, name string) string {
	b, err := dst.ReadFile(fsys, name)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

// Verifies: [full-stays-full]
// Fails if: after Fill a write grows the disk, writes other than exactly
// the bytes that fit, or fails without ENOSPC; a create succeeds with no
// byte free, or fails once space was freed; freed space can't be written;
// data written before is lost; or StopFaults leaves the cap. The negative:
// before Fill, and after StopFaults, the same writes succeed whole.
func TestFullStaysFull(t *testing.T) {
	onDisk(t, 1, simfs.Options{}, func(fsys dst.FS, disk *simworld.Machine, powerLoss func(), check func(bool, string, ...any)) {
		a, err := fsys.OpenFile("a", os.O_CREATE|os.O_RDWR, 0o644)
		check(err == nil, "create a: %v", err)
		_, err = a.Write([]byte(strings.Repeat("a", 100)))
		check(err == nil, "write before Fill: %v", err)
		check(a.Sync() == nil, "sync")
		check(dst.SyncDir(fsys, ".") == nil, "sync dir")

		disk.Disk.Fill()
		n, err := a.Write([]byte("more"))
		check(n == 0 && errors.Is(err, syscall.ENOSPC), "append to a full disk: %d, %v; want 0, ENOSPC", n, err)
		n, err = a.WriteAt([]byte(strings.Repeat("A", 50)), 0)
		check(n == 50 && err == nil, "overwrite inside the file on a full disk: %d, %v; want 50, nil", n, err)
		_, err = fsys.OpenFile("b", os.O_CREATE|os.O_RDWR, 0o644)
		check(errors.Is(err, syscall.ENOSPC), "create on a full disk: %v, want ENOSPC", err)
		check(errors.Is(fsys.Mkdir("d", 0o755), syscall.ENOSPC), "mkdir on a full disk: want ENOSPC")

		// Truncating frees 40 bytes: a 50-byte append writes exactly 40.
		check(a.Truncate(60) == nil, "truncate")
		n, err = a.WriteAt([]byte(strings.Repeat("x", 50)), 60)
		check(n == 40 && errors.Is(err, syscall.ENOSPC), "append after freeing 40 bytes: %d, %v; want 40, ENOSPC", n, err)
		check(readAll(fsys, "a") == strings.Repeat("A", 50)+strings.Repeat("a", 10)+strings.Repeat("x", 40),
			"a holds %q after the short write", readAll(fsys, "a"))

		// Removing a frees everything: a new file takes the space again.
		a.Close()
		check(fsys.Remove("a") == nil, "remove a")
		b, err := fsys.OpenFile("b", os.O_CREATE|os.O_RDWR, 0o644)
		check(err == nil, "create after freeing space: %v", err)
		n, err = b.Write([]byte(strings.Repeat("b", 100)))
		check(n == 100 && err == nil, "write into freed space: %d, %v", n, err)
		n, err = b.Write([]byte("b"))
		check(n == 0 && errors.Is(err, syscall.ENOSPC), "one byte past the cap: %d, %v", n, err)

		// Nothing already written was lost: b survives a power loss once synced.
		check(b.Sync() == nil && dst.SyncDir(fsys, ".") == nil, "sync b")
		disk.Disk.StopFaults()
		n, err = b.Write([]byte("after"))
		check(n == 5 && err == nil, "after StopFaults: %d, %v; want the cap lifted", n, err)
	})
}

// Verifies: [fsync-error-forgets]
// Fails if: after a Sync fails with EIO, the file's unsynced writes
// survive a power loss, aren't readable before it, or the next Sync fails
// too; another file's writes are lost with it; or a Sync that succeeded
// loses data. Over many seeds both outcomes must occur (non-vacuous), and
// a retried Sync must succeed while the data is still lost (the trap the
// law exists for). Half the seeds write the files back with holes, after
// a further unsynced write: the dropped data must not come back through
// a page the later write touched.
func TestFsyncErrorForgets(t *testing.T) {
	failed, kept, retriedLost := 0, 0, 0
	for seed := range uint64(200) {
		holes := seed%2 == 1
		opts := simfs.Options{SyncErrProb: 0.5}
		if holes {
			opts.HoleProb = 1
		}
		onDisk(t, seed, opts, func(fsys dst.FS, disk *simworld.Machine, powerLoss func(), check func(bool, string, ...any)) {
			files := []string{"a", "b"}
			synced := map[string]bool{}
			for _, name := range files {
				f, err := fsys.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
				check(err == nil, "create %s: %v", name, err)
				_, err = f.Write([]byte(name + "-data"))
				check(err == nil, "write %s: %v", name, err)
				f.Close()
			}
			check(dst.SyncDir(fsys, ".") == nil, "directory syncs don't fail")
			for _, name := range files {
				f, _ := fsys.OpenFile(name, os.O_RDWR, 0)
				err := f.Sync()
				switch {
				case err == nil:
					synced[name] = true
				case errors.Is(err, syscall.EIO):
					failed++
					check(readAll(fsys, name) == name+"-data", "%s isn't readable after its failed Sync: %q", name, readAll(fsys, name))
					check(f.Sync() == nil, "the Sync after a failed one failed too")
					if holes {
						_, err := f.Write([]byte("-more")) // unsynced, on the same page
						check(err == nil, "write %s: %v", name, err)
					}
				default:
					check(false, "sync %s: %v", name, err)
				}
				f.Close()
			}
			powerLoss()
			for _, name := range files {
				got := readAll(fsys, name)
				if synced[name] {
					kept++
					check(got == name+"-data", "%s's Sync succeeded but a power loss left %q", name, got)
				} else {
					retriedLost++
					check(!strings.Contains(got, name+"-data") && strings.Trim(strings.TrimSuffix(got, "-more"), "\x00") == "",
						"%s's Sync failed (then a retry succeeded) but its writes survived a power loss: %q", name, got)
				}
			}
		})
	}
	if failed == 0 || kept == 0 || retriedLost == 0 {
		t.Fatalf("vacuous: %d failed syncs, %d files kept, %d lost after a retried sync", failed, kept, retriedLost)
	}
	t.Logf("200 seeds: %d failed syncs, %d files kept by a good sync, %d lost though a retried sync succeeded", failed, kept, retriedLost)
}

// Verifies: [io-error-does-nothing]
// Fails if: a Write or Read failing with EIO transfers bytes or moves the
// offset: retrying each until it succeeds must give exactly the bytes
// written, in order. Errors must happen (non-vacuous), and be EIO.
func TestIOErrorDoesNothing(t *testing.T) {
	werr, rerr := 0, 0
	for seed := range uint64(50) {
		onDisk(t, seed, simfs.Options{WriteErrProb: 0.3, ReadErrProb: 0.3}, func(fsys dst.FS, _ *simworld.Machine, _ func(), check func(bool, string, ...any)) {
			f, err := fsys.OpenFile("f", os.O_CREATE|os.O_RDWR, 0o644)
			check(err == nil, "create: %v", err)
			var want strings.Builder
			for i := range 20 {
				chunk := fmt.Sprintf("chunk%02d;", i)
				want.WriteString(chunk)
				for {
					n, err := f.Write([]byte(chunk))
					if err == nil {
						check(n == len(chunk), "write %d of %d", n, len(chunk))
						break
					}
					check(n == 0 && errors.Is(err, syscall.EIO), "a failed write: %d, %v; want 0, EIO", n, err)
					werr++
				}
			}
			_, err = f.Seek(0, io.SeekStart)
			check(err == nil, "seek: %v", err)
			var got []byte
			buf := make([]byte, 5)
			for {
				n, err := f.Read(buf)
				if errors.Is(err, syscall.EIO) {
					check(n == 0, "a failed read returned %d bytes", n)
					rerr++
					continue
				}
				got = append(got, buf[:n]...)
				if err == io.EOF {
					break
				}
				check(err == nil, "read: %v", err)
			}
			check(string(got) == want.String(), "retrying failed writes and reads gave %q, want %q", got, want.String())
		})
	}
	if werr == 0 || rerr == 0 {
		t.Fatalf("vacuous: %d write errors, %d read errors", werr, rerr)
	}
	t.Logf("50 seeds: %d write errors and %d read errors retried", werr, rerr)
}

// TestPowerLossNamesFiles: the event log line after a power loss names
// each file that lost or tore writes, and the directory operations lost,
// by path; it names nothing that was synced. Fails if the line is missing,
// names a synced file, or misses a lost one. Not a law: a debugging aid
// (finding where a crash cut is the first question after a failure).
func TestPowerLossNamesFiles(t *testing.T) {
	var lines []string
	sim := simworld.New(simworld.Options{Seed: 1, Logf: func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}})
	disk := sim.AddMachine("disk", simworld.MachineOptions{Disk: simfs.Options{TornWriteProb: 1}})
	res := sim.Run(func(*dst.World) {
		fsys := disk.World.FS()
		fsys.MkdirAll("d", 0o755)
		dst.SyncDir(fsys, ".")
		synced, _ := fsys.OpenFile("d/synced", os.O_CREATE|os.O_RDWR, 0o644)
		synced.Write([]byte("kept"))
		synced.Sync()
		dst.SyncDir(fsys, "d")
		// Unsynced: writes to two files, and a create.
		synced.Write([]byte(strings.Repeat("x", 10)))
		other, _ := fsys.OpenFile("d/new", os.O_CREATE|os.O_RDWR, 0o644)
		other.Write([]byte(strings.Repeat("y", 10)))
		disk.Crash()
		sim.S.Restart(disk.M)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	var lost string
	for _, l := range lines {
		if strings.Contains(l, "disk of disk lost:") {
			lost = l
		}
	}
	if lost == "" {
		t.Fatalf("no line naming what the power loss dropped; log:\n%s", strings.Join(lines, "\n"))
	}
	for _, want := range []string{"d/synced (", "create d/new"} {
		if !strings.Contains(lost, want) {
			t.Errorf("the loss line doesn't name %q: %s", want, lost)
		}
	}
	t.Logf("%s", lost)
}
