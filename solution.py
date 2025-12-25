
from Pyro4 import expose
import time


class Solver(object):
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers or []
        print("Inited, workers amount: %d" % len(self.workers))

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))

        total_lines = self._count_lines(self.input_file_name)
        print("Total lines: %d" % total_lines)

        if total_lines == 0:
            self.write_output(None, None, 0, [], None, note="empty input")
            print("Job Finished (empty)")
            return

        num_workers = len(self.workers)

        if num_workers == 0:
            print("No workers, master running local")
            t0 = time.time()
            mn, mx = self._minmax_file_local(self.input_file_name)
            t1 = time.time()
            self.write_output(mn, mx, 0, [], t1 - t0, note="local master")
            print("Job Finished (local)")
            return

        print("Parallel with %d workers" % num_workers)

        # ranges
        ranges = self._make_ranges(total_lines, num_workers)

        mapped = []
        used_workers = 0
        worker_times = []
        partial_results = []

        with open(self.input_file_name, "r") as f:
            for i, (a, b) in enumerate(ranges):

                need = b - a
                lines = []
                for _ in range(need):
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line:
                        lines.append(line)

                print("map worker %d lines [%d, %d) actual=%d" % (i, a, b, len(lines)))

                partial_results.append(self.workers[i].mymap(lines))
                used_workers += 1


        mn, mx, worker_times = self.myreduce(partial_results)

        self.write_output(mn, mx, used_workers, worker_times, None, note="distributed")
        print("Job Finished")


    @staticmethod
    def _count_lines(path):
        c = 0
        with open(path, "r") as f:
            for _ in f:
                c += 1
        return c

    @staticmethod
    def _make_ranges(total, n):
        base = total // n
        rem = total % n
        out = []
        start = 0
        for i in range(n):
            extra = 1 if i < rem else 0
            end = start + base + extra
            out.append((start, end))
            start = end
        return out

    @staticmethod
    def _minmax_file_local(path):
        mn = None
        mx = None
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                for tok in line.split():
                    try:
                        v = int(tok)
                    except Exception:
                        continue
                    if mn is None or v < mn:
                        mn = v
                    if mx is None or v > mx:
                        mx = v
        return mn, mx


    @staticmethod
    @expose
    def mymap(lines):

        t0 = time.time()
        mn = None
        mx = None

        for line in lines:
            for tok in line.split():
                try:
                    v = int(tok)
                except Exception:
                    continue
                if mn is None or v < mn:
                    mn = v
                if mx is None or v > mx:
                    mx = v

        t1 = time.time()
        return (mn, mx, t1 - t0)


    @staticmethod
    @expose
    def myreduce(mapped):
        gmin = None
        gmax = None
        worker_times = []

        for idx, block in enumerate(mapped):
            value = getattr(block, "value", block)
            mn, mx, elapsed = value
            worker_times.append(elapsed)

            if mn is None and mx is None:
                continue

            if gmin is None or (mn is not None and mn < gmin):
                gmin = mn
            if gmax is None or (mx is not None and mx > gmax):
                gmax = mx

        return gmin, gmax, worker_times


    def write_output(self, mn, mx, used_workers, worker_times, master_time, note=None):
        f = open(self.output_file_name, "w")
        f.write("workers: %d\n" % used_workers)

        if used_workers > 0 and worker_times:
            parts = []
            for idx, t in enumerate(worker_times):
                parts.append("w%d=%.6f" % (idx, t))
            f.write("worker_times_sec: %s\n" % ",".join(parts))
        else:
            if master_time is not None:
                f.write("master_time_sec: %.6f\n" % master_time)

        f.write("min=%s\n" % str(mn))
        f.write("max=%s\n" % str(mx))
        f.close()
        print("output done")
