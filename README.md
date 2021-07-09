# AO-FITSScraper
This Python script takes iterates over a root folder and subfolders looking for FITS files. It opens each FITS files and reads the Headers using astropy. Then it saves the attributes into a CSV file and a MySQL database.

Imports / Dependencies
- astropy
- os
- sys
- pandas
- mariadb
- from astropy.io.fits.verify import VerifyError

# Setup Variables

- rootdir = root directory to look for FITS files, ('/share/pdata2/pdev/')
- csvdir = directory to save the output CSV file with all attributes retrieved, ('/share/jalvarad/FITSScraper/CSV/')
- tmpdir = temporary working directory, ('/share/jalvarad/FITSScraper/tmp/')
- scriptdir = directory where Python script is located, ('/share/jalvarad/FITSScraper/')
- fileList = text file containing a list of files to read, previously generated by AO-FITS_File_List.py, (scriptdir + 'pdata2/' + 'File2_2.txt')

# Database Variables
- useMysql = save the records to a MySQL/MariaDB database, True
- truncateTbl = truncate the MySQL table at the beginning of the routine, False
- truncateCSV = truncate the CSV file at the beginning of the routine, False

# Database table layout
The script will assume you have a table created with the followign columns:
- Filename = name of the file containing the attribute
- Directory = where the file was located when read
- Attribute = name of the FITS attribute read
- Value = value of the attribute
- Comments = FITS comments for the attribute
- Hdr0 = experimental: X if the attribute was read from the first Header of the FITS file
- Hdr1 = experimental: X if the attribute was read from the second Header of the FITS file

# General Execution Notes
- The script will start looking at the root directory
- It will loop through all the files and check:
  1. if the file is inside the fileList
  2. if the file has .fits extension
- After checking the files, if a valid file is found, astropy will read the FITS headers and insert into database table and CSV file