
"""
CLI for Preservica Mass Modify.

Author: Christopher Prince
license: Apache License 2.0"
"""


from preservica_modify.presModify import PreservicaMassMod
from preservica_modify.common import server_helper, fmthelper, metadata_helper
from preservica_modify.dataRetrieve import DataRetrieve, StandaloneDataRetrieve
from preservica_modify.auth import PreservicaAuth
from preservica_modify.progress import configure_cli_logging
from tqdm.contrib.logging import logging_redirect_tqdm

import argparse, os, logging
from importlib import metadata

logger = logging.getLogger(__name__)

def _get_version():
    try:
        return metadata.version("preservica_mass_modify")
    except metadata.PackageNotFoundError:
        return "0.0.0"

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog = "Preservica_Mass_Modify", description="A tool for making mass modifications to entities in Preservica based on an input spreadsheet and optional metadata files.")
    parser.add_argument("-v", "--version", action = 'version', version = '%(prog)s {version}'.format(version =_get_version()))
    parser.add_argument("input", nargs = "?",
                        help="Path to the input spreadsheet containing the modifications to be made. Must include a column for Entity Reference and Document Type (IO or Folder)")
    
    program_group = parser.add_argument_group("Modification Options", "Options for controlling the modification process. ")
                                              
    program_group.add_argument("-del", "--delete", action="store_true", default=False,
                        help="Enable deletion of entities. This will delete any entities that are listed in the input spreadsheet with DELETE in Headers row marked TRUE."\
                        "WARNING: Use this option with extreme caution, as it will permanently delete entities from your Preservica system if used incorrectly.")
    program_group.add_argument("--delete-metadata", action="store_true", default=False,
                        help="Enable deletion of metadata. This will delete any metadata for entities that are listed in the input spreadsheet." \
                        "When used with \"Delete Metadata\" in Headers row marked TRUE, it will only affect the metadata fields specified in the input spreadsheet for the corresponding entities." \
                        "When used with \"Delete All Metadata\" in Headers row marked TRUE, it will delete all metadata for the corresponding entities regardless of the fields specified in the input spreadsheet." \
                        "WARNING: Use this option with extreme caution, as it will permanently delete metadata from your Preservica system if used incorrectly.")

    program_group.add_argument("-clr","--blank-override", default = False, action="store_true",
                        help="By default, blank cells in the input spreadsheet will be ignored and will not override existing values in Preservica. "\
                        "Enabling this flag will cause blank cells to override existing values and effectively clear those fields in Preservica. "\
                        "Also affects XML metadata updates - if a field is included in the XML file with no value, it will be cleared in Preservica. "\
                        "WARNING: Use this option with caution, as it can lead to data loss if used incorrectly.")
    program_group.add_argument("-d", "--descendants", nargs = "+", choices = ["include-assets",
                                                                       "include-folders",
                                                                       "include-title",
                                                                       "include-description",
                                                                       "include-security",
                                                                       "include-retention",
                                                                       "include-xml",
                                                                       "include-identifiers"],
                        help="Include descendants in the modification process. By default, only the entities explicitly listed in the input spreadsheet will be modified. " \
                        "Enabling this option will also modify descendant entities based on the criteria specified." \
                        "For example, if 'include-assets' is specified, all asset descendants of listed folders will also be modified according to the input spreadsheet. " \
                        "Also affects metadata updates - if 'include-xml' is specified, XML metadata updates will also be applied to descendant entities. ")
    program_group.add_argument("--dummy", "--dummy-run", action="store_true",
                        help="Run the program in dummy mode (no actual changes will be made to the system, but all processing will occur as normal and a report will be generated at the end)")
    program_group.add_argument("--column-sensitivity", action="store_true",
                        help="Enable column sensitivity. By default, column names in the input spreadsheet are case sensitive, meaning that 'Title' and 'title' won't match." \
                        "Enabling this option will make column names case insensitive, so 'Title', 'title', and 'TITLE' would all be treated as the same column.")

    metadata_group = parser.add_argument_group("Metadata Options", "Options for handling XML metadata updates. " \
    "If you are not making any metadata updates, you can ignore this section. ")

    metadata_group.add_argument("-mdir","--metadata_dir", required = False, default = os.path.join(os.path.dirname(__file__), "metadata"),
                        help="Path to the directory containing any metadata files to be uploaded. " \
                        "If not specified, the program will look for metadata files in the same directory as the input spreadsheet. " \
                        "This is only relevant if you are including XML updates.")
    metadata_group.add_argument("-m", "-xml","--metadata", default = None, nargs="?", const="exact", choices=["flat","exact"], type = metadata_helper,
                        help="Method for handling XML updates. If not specified, no XML updates will be made. " \
                        "If specified without a value, the 'exact' method will be used, which flattens the XML structure to only include elements being updated. " \
                        "If 'exact' is specified, the entire XML structure must be included in the metadata directory and it will be uploaded as-is (including any elements that are not being updated). ")

    metadata_group.add_argument("--use-remote_xmls", action="store_true",
                        help="Use remote XML metadata files from the system instead of local files in the metadata directory.")
    metadata_group.add_argument("--print-xmls", action="store_true",
                        help="Print the XML metadata files in the metadata directory to the console. This is useful for verifying that the XML files are correctly formatted and can be parsed by the program before running the full modification process.")
    metadata_group.add_argument("--print-remote-xmls", action="store_true",
                        help="Print the XML metadata files on the system to the console. This is useful for verifying your XML files are correctly formatted.")
    metadata_group.add_argument("--convert-xmls", type=fmthelper, const="xlsx", nargs="?",
                        help="Convert XML metadata files in the metadata directory to a specified format (Excel, CSV, JSON, or ODS). This is useful for generating templates.")
    metadata_group.add_argument("--convert-remote-xmls", type=fmthelper, const="xlsx", nargs="?",
                        help="Convert XML metadata files on the system to a specified format (Excel, CSV, JSON, or ODS). This is useful for generating templates.")
    
    login_group = parser.add_argument_group("Authentication Options", "Options for authenticating with the Preservica server. " \
    "\nYou must provide either a credentials file or a username for authentication.\n" \
    "\nPasswords can be securely stored and retrieved using the keyring library by enabling the using the --use-keyring -- option.")
    credvsuser = login_group.add_mutually_exclusive_group(required=False)

    credvsuser.add_argument("--use-credentials", nargs='?', const=os.path.join(os.getcwd(),"credentials.properties"),
                        help="Use a credentials.properties file for authentication. " \
                        "If specified without a value, the program will look for a credentials.properties file in the current working directory." \
                        "The credentials.properties file should contain the following properties: username, password, server, tenant.")
    credvsuser.add_argument("-u", "--username", type=str,
                        help="Username for authentication with Preservica. Will prompt for password if --use-keyring is not enabled.")
    login_group.add_argument("--manager-username", type=str,
                        help="Manager Username for authentication with Preservica. Will prompt for password if --use-keyring is not enabled.")    
    login_group.add_argument("-s", "--server", type=server_helper,
                        help="URL of the Preservica server to connect to.")
    login_group.add_argument("--tenant", type=str,
                        help="Tenant name for authentication with Preservica.")
    login_group.add_argument("--use-keyring", action="store_true",
                        help="Use the keyring library to securely store and retrieve the password. "
                        "If enabled, the program will first check the specified keyring service for a stored password before prompting the user to enter their password. If a password is entered and --save-password is enabled, the password will be saved to the keyring for future use.")
    login_group.add_argument("--save-password", action="store_true",  
                        help="When used in conjunction with --use-keyring, this option will save the password entered by the user to the specified keyring service for future use. Use this option with caution, as it will store your password in the keyring.")
    login_group.add_argument("--keyring-service", type=str, default="preservica_modify",
                        help="The name of the keyring service to use for storing/retrieving the password if --use-keyring --save-password is enabled. Default is 'preservica_modify'.")
    login_group.add_argument("--test-login", action="store_true",
                        help="Test login credentials and exit. This will attempt to log in to Preservica using the provided credentials (either through command line arguments or a credentials file) and then exit the program. This is useful for verifying that your credentials are correct and that you can connect to the Preservica server before running the full modification process.")

    config_group = parser.add_argument_group("Configuration Options", "Additional options for configuring the program. ")

    config_group.add_argument("-opt", "--options-file", type=str, default=os.path.join(os.path.dirname(__file__),'options','options.properties'),
                        help="Path to the options.properties file containing any additional configuration options. If not specified, the program will look for an options.properties file in the same directory as the script.")
    config_group.add_argument("--no-checkpoint-hash", dest="checkpoint_hash", action="store_false",
                        help="Disable input hashing for resume checkpoints. Changes to the input file will not be detected.")
    config_group.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
                        help="Set the logging level")
    config_group.add_argument("--log-file", nargs='?', default=None, help="Also write logs to this file; logs remain visible in the terminal.")

    return parser 


def run_cli(args: argparse.Namespace | None = None) -> None:

    if args is None:
        parser = create_parser()
        args = parser.parse_args()

    try:
        log_level = getattr(logging, args.log_level.upper()) if args.log_level else logging.INFO
    except Exception:
        log_level = logging.INFO
    log_format = '%(asctime)s %(levelname)-8s [%(name)s] %(message)s'
    configure_cli_logging(log_level, args.log_file, log_format)
    logger.debug(f'Logging configured (level={logging.getLevelName(log_level)}, file={args.log_file or "stdout"})')

    if args.print_xmls:
        DataRetrieve(args.metadata_dir).print_local_xmls()
        logger.info("XML metadata printed successfully! Exiting program.")
        raise SystemExit()

    if args.convert_xmls:
        DataRetrieve(args.metadata_dir).convert_local_xmls(args.convert_xmls)
        logger.info("XML metadata converted successfully! Exiting program.")
        raise SystemExit()

    if not args.use_credentials and not args.server:
        msg = "Server not provided. Please provide either a credentials file or a server URL for authentication, closing program..."
        logger.error(msg)
        raise ValueError(msg)
    if not args.use_credentials and not args.username:
        msg = "No authentication method provided. Please provide either a credentials file or a username for authentication, closing program..."
        logger.error(msg)
        raise ValueError(msg)

    if args.test_login:
        try:
            PreservicaAuth(server=args.server,
                           username=args.username,
                           tenant=args.tenant,
                           credentials_file=args.use_credentials,
                           manager_username=args.manager_username,
                           use_keyring=args.use_keyring,
                           save_password_to_keyring=args.save_password,
                           keyring_service=args.keyring_service).login_preservica()
            logger.info("Login successful!")
            raise SystemExit()
        except Exception:
            logger.exception("Login failed!")
            raise

    if args.print_remote_xmls:
        StandaloneDataRetrieve(args.metadata_dir, server=args.server, username=args.username,
                               tenant=args.tenant, credentials_file=args.use_credentials,
                               use_keyring=args.use_keyring, save_password_to_keyring=args.save_password,
                               keyring_service=args.keyring_service).print_remote_xmls()
        logger.info("Remote XML metadata printed successfully! Exiting program.")
        raise SystemExit()
    
    if args.convert_remote_xmls:
        StandaloneDataRetrieve(args.metadata_dir, server=args.server, username=args.username,
                               tenant=args.tenant, credentials_file=args.use_credentials,
                               use_keyring=args.use_keyring, save_password_to_keyring=args.save_password,
                               keyring_service=args.keyring_service).convert_remote_xmls(args.convert_remote_xmls)
        logger.info("Remote XML metadata printed successfully! Exiting program.")
        raise SystemExit()

    if not os.path.isfile(os.path.abspath(args.input)):
        logger.error("Invlaid file selected for input, closing program...")
        raise FileNotFoundError("Invalid input file")

    if args.delete and not (args.manager_username or args.use_credentials):
        msg = "Delete Option requires a Manager Username or Credentials file for authentication. Please provide a credentials file or remove the delete option."
        logger.error(msg)
        raise ValueError(msg)

    if args.metadata_dir is not None:
        if not os.path.isdir(os.path.abspath(args.metadata_dir)):
            msg = "Invlaid folder selected for metadata directory, closing program..."
            logger.error(msg)
            raise NotADirectoryError(msg)
    if args.descendants:
        if not any(x in ["include-assets","include-folders"] for x in args.descendants):
            msg = 'Descendants must include either "include-assets" or "include-folders"'
            logger.error(msg)
            raise ValueError(msg)
        if "include-title" in args.descendants:
            i = input("WARNING: You are about to update the Title field of all descendants." \
            "\nThis is not something you would normally do and it can potentially lead to corruption of your database...\nPlease confirm you wish to continue by entering Y: ")
            if not i.lower() == "y":
                logger.info("You have chosen not to proceed... Good choice. Closing Program")
                raise SystemExit()

    with logging_redirect_tqdm():
        PreservicaMassMod(input_file=args.input,
                          metadata_dir=args.metadata_dir,
                          delete_metadata_flag=args.delete_metadata,
                          blank_override=args.blank_override,
                          metadata=args.metadata,
                          descendants=args.descendants,
                          username=args.username,
                          manager_username=args.manager_username,
                          server=args.server,
                          delete=args.delete,
                          tenant=args.tenant,
                          dummy=args.dummy,
                          options_file=args.options_file,
                          credentials=args.use_credentials,
                          use_keyring=args.use_keyring,
                          keyring_service=args.keyring_service,
                          save_password_to_keyring=args.save_password,
                          column_sensitivity=args.column_sensitivity,
                          use_remote_xmls=args.use_remote_xmls,
                          checkpoint_hash=args.checkpoint_hash,
                          show_progress=True
                          ).main()


def main() -> None:
    try:
        parser = create_parser()
        args = parser.parse_args()
        run_cli(args)
    except KeyboardInterrupt:
        raise SystemExit()
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise
        
if __name__ == "__main__":
    main()