Preservica Mass Modify - Portable Edition
===========================================

This is a standalone, portable distribution of Preservica Mass Modify.

Installation
============

For Windows:
1. Extract the ZIP file to your desired location
2. Navigate to the preservica_mass_modify folder
3. Run install.cmd (right-click and select "Run as Administrator")
4. Follow the on-screen instructions

After installation, you can use preservica_modify command from any command prompt.

Usage
=====

Basic usage:
  preservica_modify -i /path/to/spreadsheet.xlsx [options]

For full options:
  preservica_modify --help

Examples
========

Login with prompted password:

preservica_modify -i /path/to/spreadsheet.xlsx -u your@username.com -s yourtenant.preservica.com

Login with Keyring:

preservica_modify -i /path/to/spreadsheet.xlsx -u your@username.com -s yourtenant.preservica.com --use-keyring

Login with Credentials:

preservica_modify -i /path/to/spreadsheet.xlsx --use-crendetials /path/to/credentials.properties

Enable Metadata

perservica_modify -i /path/to/spreadsheet.csv -m exact [...]

Enable Blank Override (Blank Cells delete data)

perservica_modify -i /path/to/spreadsheet.csv -clr [...]

Uninstallation
==============

To uninstall from Windows:
1. Navigate to the preservica_mass_modify folder
2. Run uninstall.cmd (right-click and select "Run as Administrator")
3. Follow the on-screen instructions

Support
=======

For more information, visit the project repository or consult the documentation.

This executable was built using Nuitka and includes all necessary dependencies.
No additional Python installation is required.
