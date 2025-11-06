
import sys, logging
from logging.handlers import RotatingFileHandler
from tqdm import tqdm

class TqdmStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_logging(*, level: str="INFO", quiet: bool=False, log_file: str|None=None, use_tqdm_handler: bool=True):
    log = logging.getLogger("netops")
    log.setLevel(level.upper())
    for h in list(log.handlers):
        log.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    if not quiet:
        h = TqdmStreamHandler() if use_tqdm_handler else logging.StreamHandler(stream=sys.stderr)
        h.setLevel(level.upper())
        h.setFormatter(fmt)
        log.addHandler(h)

    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setLevel(level.upper())
        fh.setFormatter(fmt)
        log.addHandler(fh)

    return log

def get_logger():
    return logging.getLogger("netops")
