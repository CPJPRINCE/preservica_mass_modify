
"""
Preservica Mass Modification tool.

CLI for PresMM

Author: Christopher Prince
license: Apache License 2.0"
"""


from preservica_modify.pres_modify import PreservicaMassMod
import argparse
import os, time
import getpass
import logging

logger = logging.getLogger(__name__)

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", required = True,
                        help="Path to the input spreadsheet containing the modifications to be made. Must include a column for Entity Reference and Document Type (IO or Folder)")
    parser.add_argument("-mdir","--metadata_dir", required = False,
                        help="Path to the directory containing any metadata files to be uploaded. " \
                        "If not specified, the program will look for metadata files in the same directory as the input spreadsheet. " \
                        "This is only relevant if you are including XML updates or using upload mode.")
    parser.add_argument("-xml","--xml_method", default = None, nargs="?", const="flat", choices=["flat","exact"],
                        help="Method for handling XML updates. If not specified, no XML updates will be made. " \
                        "If specified without a value, the 'flat' method will be used, which flattens the XML structure to only include elements being updated. " \
                        "If 'exact' is specified, the entire XML structure must be included in the metadata directory and it will be uploaded as-is (including any elements that are not being updated). ")
    parser.add_argument("-clr","--blank-override", default = False, action="store_true",
                        help="By default, blank cells in the input spreadsheet will be ignored and will not override existing values in Preservica. "\
                        "Enabling this flag will cause blank cells to override existing values and effectively clear those fields in Preservica. "\
                        "WARNING: Use this option with caution, as it can lead to data loss if used incorrectly.")
    parser.add_argument("-del", "--delete", action="store_true",
                        help="Enable deletion of entities. This will delete any entities that are listed in the input spreadsheet with DELETE in Headers row marked TRUE."\
                        "WARNING: Use this option with extreme caution, as it will permanently delete entities from your Preservica system if used incorrectly.")
    parser.add_argument("-up", "--upload-mode", action="store_true",
                        help="Enable upload mode. This will create new entities in Preservica based on the information provided in the input spreadsheet and any associated metadata files in the metadata directory. "\
                        "WANRING: EXPERIMENTAL FEATURE. Use with caution.")
    parser.add_argument("-d", "--descendants", nargs = "+", choices = ["include-assets",
                                                                       "include-folders",
                                                                       "include-title",
                                                                       "include-description",
                                                                       "include-security",
                                                                       "include-retention",
                                                                       "include-xml",
                                                                       "include-identifiers"],
                        help="Include descendants in the modification process. By default, only the entities explicitly listed in the input spreadsheet will be modified. " \
                        "Enabling this option will also modify descendant entities based on the criteria specified." \
                        "For example, if 'include-assets' is specified, all asset descendants of listed folders will also be modified according to the input spreadsheet. ")
    
    mgroup = parser.add_mutually_exclusive_group(required=True)

    mgroup.add_argument("-u", "--username", type=str,
                        help="Username for authentication with Preservica. Not required if using --use-credentials option.")
    parser.add_argument("-s", "--server", type=server_helper,
                        help="URL of the Preservica server to connect to. Not required if using --use-credentials option.")
    parser.add_argument("--tenant", type=str,
                        help="Tenant name for authentication with Preservica. Not required if using --use-credentials option.")
    parser.add_argument("-opt", "--options-file", type=str, default=os.path.join(os.path.dirname(__file__),'options','options.properties'),
                        help="Path to the options.properties file containing any additional configuration options. If not specified, the program will look for an options.properties file in the same directory as the script.")
    mgroup.add_argument("--use-credentials", nargs='?', const=os.path.join(os.getcwd(),"credentials.properties"),
                        help="Use a credentials.properties file for authentication. " \
                        "If specified without a value, the program will look for a credentials.properties file in the current working directory." \
                        "The credentials.properties file should contain the following properties: username, password, server, tenant.")
    parser.add_argument("--dummy", action="store_true",
                        help="Run the program in dummy mode (no actual changes will be made to the system, but all processing will occur as normal and a report will be generated at the end)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
                        help="Set the logging level")
    parser.add_argument("--log-file", nargs='?', default=None, help="Path to a file to write logs to. If not specified, logs will be written to stdout.")
    
    return parser 


def run_cli(args: argparse.Namespace) -> None:

    try:
        log_level = getattr(logging, args.log_level.upper()) if args.log_level else logging.INFO
    except Exception:
        log_level = logging.INFO
    log_format = '%(asctime)s %(levelname)-8s [%(name)s] %(message)s'
    if args.log_file:
        logging.basicConfig(level=log_level, filename=args.log_file, filemode='a', format=log_format)
    else:
        logging.basicConfig(level=log_level, format=log_format)
    logger.debug(f'Logging configured (level={logging.getLevelName(log_level)}, file={args.log_file or "stdout"})')

    if not os.path.isfile(os.path.abspath(args.input)) or not args.input.endswith((".xlsx",".csv")):
        print("Invlaid file selected for input, closing program...")
        time.sleep(5)
        raise SystemExit()
    if args.metadata_dir is not None:
        if not os.path.isdir(os.path.abspath(args.metadata_dir)):
            print("Invlaid folder selected for metadata directory, closing program...")
            time.sleep(5)
            raise SystemExit()
    if args.username:
        passwd = getpass.getpass(prompt="Please enter your password for Preservica: ")
    if args.descendants:
        if not any(x in ["include-assets","include-folders"] for x in args.descendants):
            print('Descendants must include either "include-assets" or "include-folders"')
            time.sleep(5)
            raise SystemExit()
        if "include-title" in args.descendants:
            i = input("WARNING: You are about to update the Title field of all descendants.\nThis is not something you would normally do and it can potentially lead to corruption of your database...\nPlease confirm you wish to continue by entering Y: ")
            if not i.lower() == "y":
                print("You have chosen not to proceed... Good choice. Closing Program")
                time.sleep(5)
                raise SystemExit()
        
    PreservicaMassMod(input_file=args.input,
                      metadata_dir=args.metadata_dir,
                      blank_override=args.blank_override,
                      xml_method=args.xml_method,
                      descendants=args.descendants,
                      username=args.username,
                      password=passwd if args.username else None,
                      server=args.server,
                      delete=args.delete,
                      tenant=args.tenant,
                      dummy=args.dummy,
                      upload_mode=args.upload_mode,
                      options_file=args.options_file,
                      credentials=args.use_credentials).main()
  
def server_helper(server_str: str) -> str:
    if server_str.startswith("http://") or server_str.startswith("https://"):
        return server_str.replace("http://","").replace("https://","")
    return server_str

def main() -> None:
    try:
        parser = create_parser()
        args = parser.parse_args()
        run_cli(args)
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user, exiting...")
        
if __name__ == "__main__":
    main()