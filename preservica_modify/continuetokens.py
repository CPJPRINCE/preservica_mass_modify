import os
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

class ContinueTokenHandler:
    def __init__(self):
        pass
    def _save_continue_token(self, token_file: str, token: Optional[int]) -> None:
        try:
            token_file = token_file + "_continue.txt"
            if token is not None:
                with open(token_file, 'w') as f:
                    f.write(str(token))
                logger.info(f'Continue token saved to {token_file}')
        except FileNotFoundError:
            logger.exception(f'Continue token file not found: {token_file}, unable to save continue token...')
            raise
        except PermissionError as e:
            logger.exception(f'Permission denied when saving continue token to file {token_file}: {e}')
            raise
        except Exception as e:
            logger.exception(f'Failed to save continue token to file {token_file}: {e}')
            raise

    def _load_continue_token(self, token_file: str) -> Optional[int]:
        try:
            token_file = token_file + "_continue.txt"
            if os.path.exists(token_file):
                with open(token_file, 'r') as f:
                    token_str = f.read().strip()
                    token = int(token_str)
                    if isinstance(token, int):
                        logger.info(f'Continue token loaded from {token_file}, processing from index: {token}')
                        return token
                    else:
                        logger.warning(f'Invalid continue token value in {token_file}: {token_str}')
                        raise ValueError(f'Invalid continue token value in {token_file}: {token_str}')
            else:
                logger.debug(f'No continue token file found at {token_file}, starting from beginning...')
                return 0
        except PermissionError:
            logger.warning(f'Permission denied when loading token {token_file}, starting from beggining')
            return 0
        except Exception:
            logger.warning(f'Invalid continue token value in {token_file}, starting from beginning...')
            return 0
                        
    def _remove_continue_token(self, token_file: str) -> None:
        token_file = token_file + "_continue.txt"
        if os.path.isfile(token_file):
            os.remove(token_file)
            logger.info(f'Continue token file {token_file} removed.')


    def _process_continue_token(self, data_dict: dict) -> tuple[List[int], int]:
        if self.disable_continue is True:
            start_idx = 0
        else:
            start_idx = self._load_continue_token(self.input_file)
            if not isinstance(start_idx, int):
                logger.error(f'Invalid continue token: {start_idx}, must be an integer index. Please ensure the continue token file contains a valid integer index.')
                raise ValueError(f'Invalid continue token: {start_idx}, must be an integer index. Please ensure the continue token file contains a valid integer index.')
            keys = list(data_dict.keys())
            if start_idx in keys:
                start_pos = keys.index(start_idx)
            else:
                start_pos = max(0, int(start_idx))
        keys = list(data_dict.keys())
        if start_idx in keys:
            start_pos = keys.index(start_idx)
        else:
            start_pos = max(0, int(start_idx))
        return keys, start_pos    
