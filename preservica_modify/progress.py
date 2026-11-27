from contextlib import contextmanager
import logging
import os
import sys
from typing import Callable, Iterator

from pyPreservica.uploadAPI import UploadProgressCallback
from tqdm import tqdm


def configure_cli_logging(level: int, log_file: str | None, log_format: str) -> None:
    logging.basicConfig(level=level, format=log_format)
    if not log_file:
        return

    root = logging.getLogger()
    log_path = os.path.normcase(os.path.abspath(log_file))
    if not any(
        isinstance(handler, logging.FileHandler)
        and os.path.normcase(handler.baseFilename) == log_path
        for handler in root.handlers
    ):
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(file_handler)
    if not any(
        isinstance(handler, logging.StreamHandler)
        and handler.stream in (sys.stderr, sys.stdout)
        for handler in root.handlers
    ):
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(log_format))
        root.addHandler(console)


def row_progress(total: int, initial: int, description: str, enabled: bool) -> tqdm:
    return tqdm(
        total=total,
        initial=initial,
        desc=description,
        unit="row",
        dynamic_ncols=True,
        colour="cyan",
        file=sys.stderr,
        disable=not enabled or not sys.stderr.isatty(),
    )


@contextmanager
def upload_progress(path: str, enabled: bool) -> Iterator[Callable[[int], None]]:
    if not enabled:
        yield UploadProgressCallback(path)
        return

    with tqdm(
        total=os.path.getsize(path),
        desc=f"Transfer: {os.path.basename(path)}",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        dynamic_ncols=True,
        colour="green",
        position=1,
        leave=False,
        file=sys.stderr,
        disable=not sys.stderr.isatty(),
    ) as progress:
        def update(amount: int) -> None:
            progress.update(amount)

        yield update
