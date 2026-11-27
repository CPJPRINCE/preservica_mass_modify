from pyPreservica import EntityAPI, RetentionAPI, UploadAPI, WorkflowAPI, AdminAPI
import logging
from typing import Optional
from getpass import getpass
try:
    import keyring
    from keyring.errors import KeyringError
except ImportError:
    keyring = None

logger = logging.getLogger(__name__)

class PreservicaAuth:
    def __init__(self, server: Optional[str], username: Optional[str], tenant: Optional[str] = None, credentials_file: Optional[str] = None, use_keyring: bool = False, save_password_to_keyring: bool = False, keyring_service: str = "preservica_modify", manager_username: Optional[str] = None, manager_password: Optional[str] = None):
        self.server = server
        self.username = username
        self.tenant = tenant
        self.credentials_file = credentials_file
        self.use_keyring = use_keyring
        self.save_password_to_keyring = save_password_to_keyring
        self.password = None
        self.manager_username = manager_username
        self.manager_password = manager_password
        self.entity = None
        self.retention = None
        self.upload = None
        self.workflow = None
        self.admin = None
        self.keyring_service = keyring_service

    def _keyring_entry_name(self) -> str:
        tenant = self.tenant or "default"
        server = self.server or "default-server"
        return f"{self.keyring_service}:{server}:{tenant}"

    def _get_password_from_keyring(self, username) -> Optional[str]:
        if not self.use_keyring:
            return None
        if keyring is None:
            raise RuntimeError("keyring package is not installed. Install with: pip install keyring")
        if not username or not self.server:
            return None
        try:
            return keyring.get_password(self._keyring_entry_name(), str(username))
        except KeyringError as e:
            logger.warning(f"Unable to read password from keyring: {e}")
            return None
        
    def _set_password_in_keyring(self, username: str, password: str) -> None:
        if not self.save_password_to_keyring:
            return
        if keyring is None:
            logger.error("keyring package is not installed. Install with: pip install keyring")
            raise RuntimeError("keyring package is not installed. Install with: pip install keyring")
        if not self.username or not self.server:
            return
        try:
            keyring.set_password(self._keyring_entry_name(), str(username), password)
            logger.info("Password saved to keyring.")
        except KeyringError as e:
            logger.warning(f"Unable to save password to keyring: {e}")

    def login_preservica(self):
        """
        Logs into Preservica. Either through manually logging in.
        """
        try:
            if self.credentials_file:
                logger.info('Using credentials file.')
                self.entity = EntityAPI(credentials_path=self.credentials_file)
                self.retention = RetentionAPI(credentials_path=self.credentials_file)
                self.upload = UploadAPI(credentials_path=self.credentials_file)
                self.workflow = WorkflowAPI(credentials_path=self.credentials_file)
                self.admin = AdminAPI(credentials_path=self.credentials_file)
                logger.info(f'Successfully logged into Preservica Server {self.server}, as user: {self.username}')
                return
                        
            def _check_password(username: str, password: Optional[str]):

                if None in (username, self.server):
                    logger.error('A Username or Server has not been provided... Please try again...')
                    raise ValueError('A Username or Server has not been provided... Please try again...')

                if password is None and self.save_password_to_keyring is False:
                    password = self._get_password_from_keyring(username)
                
                if password is None:
                    password = getpass(prompt=f"Please enter your password for Preservica for {username}: ")
                    if self.save_password_to_keyring is True:
                        self._set_password_in_keyring(username, password)
                
                if password is not None:
                    return password
                else:
                    logger.error('Password not provided and could not be retrieved from keyring. Please try again...')
                    raise ValueError('Password not provided and could not be retrieved from keyring. Please try again...')
            
            if self.username:
                self.password = _check_password(self.username, self.password)

            if self.manager_username:
                self.manager_password = _check_password(self.manager_username, self.manager_password)

            self.entity = EntityAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.retention = RetentionAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.upload = UploadAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.workflow = WorkflowAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            self.admin = AdminAPI(username=str(self.username), password=str(self.password),server=str(self.server), tenant=str(self.tenant) if self.tenant else None)
            logger.info(f'Successfully logged into Preservica Server {self.server}, as user {self.username}')
        except Exception:
            logger.exception('Failed to login to Preservica')
            raise

    def test_login(self):
        """
        Test Login function, to ensure credentials are correct before running main.
        """
        try:
            self.login_preservica()
            logger.info(f'Login successful: {self.server} as {self.username}')
        except Exception:
            logger.exception('Login failed')
            raise
