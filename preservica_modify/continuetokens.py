import hashlib
import json
import logging
import os
import tempfile
from typing import Literal, Optional


logger = logging.getLogger(__name__)


class ContinueTokenHandler:
    def _input_hash(self) -> Optional[str]:
        if not self.checkpoint_hash:
            logger.warning("Checkpoint input hashing is disabled; input changes will not be detected.")
            return None
        digest = hashlib.sha256()
        with open(self.input_file, "rb") as input_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _save_checkpoint(
        self,
        operation: Literal["modify", "upload"],
        position: int,
        in_progress: bool,
        input_hash: Optional[str],
        last_ref: Optional[str] = None,
    ) -> None:
        checkpoint_file = self.input_file + f"_{operation}_continue.json"
        checkpoint = {
            "position": position,
            "last_ref": last_ref,
            "in_progress": in_progress,
            "input_hash": input_hash,
            "dummy": self.dummy_flag,
        }
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=os.path.dirname(os.path.abspath(checkpoint_file)),
                prefix=f".{operation}-checkpoint-", delete=False,
            ) as temp_file:
                temp_path = temp_file.name
                json.dump(checkpoint, temp_file)
                temp_file.flush()
                os.fsync(temp_file.fileno())
            os.replace(temp_path, checkpoint_file)
        finally:
            if temp_path is not None and os.path.exists(temp_path):
                os.remove(temp_path)

    def _load_checkpoint(
        self, operation: Literal["modify", "upload"], keys: list, input_hash: Optional[str]
    ) -> tuple[int, Optional[str]]:
        legacy_file = self.input_file + "_continue.txt"
        if os.path.exists(legacy_file):
            raise RuntimeError(
                f"Legacy continue token is not supported: {legacy_file}. "
                "Check previous progress and resolve the token before starting."
            )

        checkpoint_file = self.input_file + f"_{operation}_continue.json"
        if not os.path.exists(checkpoint_file):
            return 0, None

        with open(checkpoint_file, encoding="utf-8") as checkpoint_file_handle:
            checkpoint = json.load(checkpoint_file_handle)
        if not isinstance(checkpoint, dict):
            raise ValueError(f"Invalid {operation} checkpoint: {checkpoint_file}")
        position = checkpoint.get("position")
        last_ref = checkpoint.get("last_ref")
        in_progress = checkpoint.get("in_progress")
        stored_hash = checkpoint.get("input_hash")
        if (type(position) is not int or not 0 <= position <= len(keys)
                or (last_ref is not None and (not isinstance(last_ref, str) or not last_ref.strip()))
                or (operation == "modify" and last_ref is not None)
                or "input_hash" not in checkpoint
                or (stored_hash is not None and not isinstance(stored_hash, str))
                or not isinstance(in_progress, bool)
                or (in_progress and position == len(keys))):
            raise ValueError(f"Invalid {operation} checkpoint: {checkpoint_file}")
        if input_hash is not None and stored_hash is None:
            raise ValueError(
                f"Cannot verify {operation} checkpoint created without input hashing: {checkpoint_file}. "
                "Check the input before resuming with hashing disabled."
            )
        if input_hash is not None and stored_hash != input_hash:
            raise ValueError(f"Input file changed since {operation} checkpoint was saved: {checkpoint_file}")
        if checkpoint.get("dummy") is not self.dummy_flag:
            raise ValueError(f"{operation.capitalize()} checkpoint dummy mode does not match the current run: {checkpoint_file}")
        if in_progress:
            raise RuntimeError(
                f"{operation.capitalize()} row {keys[position]} may have been partially processed. "
                f"Check Preservica and resolve the checkpoint before resuming: {checkpoint_file}"
            )
        return position, last_ref

    def _remove_checkpoint(self, operation: Literal["modify", "upload"]) -> None:
        checkpoint_file = self.input_file + f"_{operation}_continue.json"
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
