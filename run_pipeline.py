"""Execute BNPL notebooks in order, in fresh kernels using this Python environment."""
import argparse
import os
from pathlib import Path
import sys
import time
import nbformat
from jupyter_client import KernelManager
from nbclient import NotebookClient

def main():
    names = ["etl", "fraud", "features", "forecast", "ranking", "summary"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", choices=names, default="etl",
                        help="Resume only when preceding outputs are already current.")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    root = Path(__file__).resolve().parent
    os.chdir(root)
    for key in ["OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"]:
        os.environ[key] = str(args.threads)
    os.environ["MASTER"] = f"local[{args.threads}]"
    os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
    (root / "tmp").mkdir(exist_ok=True)
    for name in names[names.index(args.start):]:
        source = root / f"{name}.ipynb"
        notebook = nbformat.read(source, as_version=4)
        km = KernelManager(kernel_name="python3")
        km.kernel_spec.argv = [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"]
        start = time.monotonic()
        print(f"Running {source.name}", flush=True)
        client = NotebookClient(notebook, km=km, timeout=1800,
                                resources={"metadata": {"path": str(root)}})
        try:
            client.execute()
        except Exception:
            nbformat.write(notebook, root / "tmp" / f"{name}.failed.ipynb")
            raise
        finally:
            if km.has_kernel:
                km.shutdown_kernel(now=True)
            km.cleanup_resources()
        staged = root / "tmp" / f"{name}.executed.ipynb"
        nbformat.write(notebook, staged)
        staged.replace(source)
        print(f"Completed {source.name} in {time.monotonic() - start:.0f}s", flush=True)
    print("All requested notebooks and deliverable checks completed.", flush=True)

if __name__ == "__main__":
    main()
