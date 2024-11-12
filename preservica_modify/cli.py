
"""
Preservica Mass Modification tool.

CLI for PresMM

Author: Christopher Prince
license: Apache License 2.0"
"""


from pres_modify import PreservicaMassMod
import argparse
import os, time

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", required = True)
    parser.add_argument("-mdir","--metadata_dir", required = True)
    parser.add_argument("-x","--xml_method", default = "flat", choices=["flat","exact"])
    parser.add_argument("-clr","--blank-override", default = False, action="store_true")
    parser.add_argument("-del", "--delete", action="store_true")
    parser.add_argument("-d", "--descendants", nargs = "+", choices = ["include-assets",
                                                                       "include-folders",
                                                                       "include-title",
                                                                       "include-description",
                                                                       "include-security",
                                                                       "include-retention",
                                                                       "include-xml",
                                                                       "include-identifier"])
    mgroup = parser.add_mutually_exclusive_group(required=True)
    mgroup.add_argument("-u", "--username", type=str)
    parser.add_argument("-p", "--password", type=str)
    parser.add_argument("-s", "--server", type=str)
    parser.add_argument("--tenant", type=str)
    parser.add_argument("-opt", "--options-file", type=str, default=os.path.join(os.path.dirname(__file__),'options.properties'))
    mgroup.add_argument("--use-credentials", nargs='?', default=os.path.join(os.getcwd(),"credentials.properties"))
    parser.add_argument("--dummy", action="store_true")
    args = parser.parse_args()
    return args

def run_cli():
    args = parse_args()
    if not os.path.isfile(os.path.abspath(args.input)) or not args.input.endswith((".xlsx",".csv")):
        print("Invlaid file selected for input, closing program...")
        #time.sleep(5)
        #raise SystemExit()
    if not os.path.isdir(os.path.abspath(args.metadata_dir)):
        print("Invlaid folder selected for metadata directory, closing program...")
        #time.sleep(5)
        #raise SystemExit()
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
        
    PreservicaMassMod(excel_file=args.input,
                            metadata_dir=args.metadata_dir,
                            blank_override=args.blank_override,
                            xml_method=args.xml_method,
                            descendants=args.descendants,
                            username=args.username,
                            password=args.password,
                            server=args.server,
                            tenant=args.tenant,
                            dummy=args.dummy,
                            options_file=args.options_file,
                            credentials=args.use_credentials).main()

if __name__ == "__main__":
    run_cli()