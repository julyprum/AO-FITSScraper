from astropy.io import fits
import os
import sys, getopt
import pandas as pd
from astropy.io.fits.verify import VerifyError


def main(argv):
    print(argv)
    inputFile = ''
    trunct = False
    truncc = False
    try:
        opts, args = getopt.getopt(argv, "p:t:c:i:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-p":
            pdata = arg
            print("pdata folder: " + arg)
        if opt == "-t":
            trunct = arg
            print("truncate table: " + arg)
        if opt == "-c":
            truncc = arg
            print("truncate csv" + arg)
        if opt == "-i":
            inputFile = arg
            print("input file: " + arg)

            rootdir = '/share/'+pdata+'/pdev/'
            csvdir = '/share/jalvarad/FITSScraper/CSV/'
            tmpdir = '/share/jalvarad/FITSScraper/tmp/'
            scriptdir = '/share/jalvarad/FITSScraper/'
            fileList = scriptdir + pdata + '/' + inputFile

            # Trigger to use Mysql Inserts
            useMysql = True
            truncateTbl = trunct
            truncateCSV = truncc

            # If Mysql is enable, create connection
            if useMysql:
                import mariadb

                try:
                    conn = mariadb.connect(
                        user="catalog_admin",
                        password="naic305c",
                        host="aolcp",
                        port=3306,
                        database="data_catalog"
                    )
                    if truncateTbl:
                        cursor = conn.cursor()
                        sql = "TRUNCATE TABLE fits_catalog"
                        cursor.execute(sql)
                        conn.commit()
                        conn.close()
                        print("fits_catalog table truncated")
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)

            # Function to be called to insert data to Mysql
            def mysql_inserts_vals(vals):
                if useMysql:
                    try:
                        conn = mariadb.connect(
                            user="catalog_admin",
                            password="naic305c",
                            host="aolcp",
                            port=3306,
                            database="data_catalog"
                        )
                        sql = "INSERT INTO fits_catalog (Filename, Directory, Attribute, Value, Comments, Hdr0, Hdr1) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor = conn.cursor()
                        cursor.execute(sql, vals)
                        conn.commit()
                        conn.close()
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)

            def mysql_inserts_sql(sql):
                if useMysql:
                    print(sql)
                    try:
                        conn = mariadb.connect(
                            user="catalog_admin",
                            password="naic305c",
                            host="aolcp",
                            port=3306,
                            database="data_catalog"
                        )
                        cursor = conn.cursor()
                        cursor.execute(sql)
                        conn.commit()
                        conn.close()
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)

            # Function to close Mysql Connections
            def mysql_close_vars():
                if useMysql:
                    conn.close()

            #Create array for dataframe
            columns = ['File Name','Directory','Attribute','Value','Attribute Comments','Hdr0','Hdr1']
            values = []

            # Prepare CSV and create File to be appended later
            # If file exists, delete and recreate
            if truncateCSV:
                if os.path.exists(csvdir + 'FITSScraperCSV' + '.csv'):
                    os.remove(csvdir + 'FITSScraperCSV' + '.csv')
                    print("File removed: " + csvdir + 'FITSScraperCSV' + '.csv')
                else:
                    print("The file does not exist")
                dataFrame = pd.DataFrame(values, columns = columns)
                dataFrame.to_csv (csvdir + 'FITSScraperCSV' + '.csv', index = False, header=True)

            # Loop through the folders/files
            for subdir, dirs, files in os.walk(rootdir):

                for file in files:

                    # Full path of file
                    fileFullLoc = os.path.join(subdir, file)

                    # If File Size = 0, Skip File
                    file_size = os.path.getsize(fileFullLoc)

                    # Use a file with a list of file names, useful to split in batches
                    # Check if File name exists inside text file (splitting long directories in batches)
                    txtFile = open(fileList)
                    #print(txtFile.read())
                    search_word = file
                    if(search_word in txtFile.read()):

                        # If File is in batch file, check if Size > 0
                        if file_size > 0 and file.endswith('.fits'):

                            #print(os.path.join(subdir, file))

                            print(file)
                            print("------------------------")

                            #Open FITS File
                            hdul = fits.open(fileFullLoc)
                            while True:
                                try:
                                    #hdul = fits.open(fileFullLoc)
                                    break
                                except OSError:
                                    #print(file + " Empty or corrupt FITS file")
                                    pass
                                except IndexError:
                                    #print(file + " has no header1")
                                    pass

                            #hdul.info()

                            # Clear the Values array
                            values = []
                            value = []
                            content = ""
                            sql = ""

                            #Iterate over Header0
                            #hdul.verify('ignore')
                            hdr = hdul[0].header
                            for card in hdr:
                                #Get Column value and append to values array
                                value = [file, subdir, card, str(hdr[card]), str(hdr.comments[card]), 'X','']
                                values.append(value)

                                #MySQL Inserts
                                #sql += "INSERT INTO fits_catalog (Filename, Directory, Attribute, Value, Comments, Hdr0, Hdr1) VALUES ('"+file+"', '"+subdir+"', '"+card+"', '"+str(hdr[card])+"', '"+str(hdr.comments[card])+"', 'X', '');"
                                mysql_inserts_vals(value)

                            #Read Header from Location 1
                            while True:
                                try:
                                    hdr1 = hdul[1].header
                                    #print("Count: " + str(len(hdr1)))
                                    #Iterate over Header1
                                    for card in hdr1:
                                        try:
                                            #print(card)
                                            #Get Column value and append to values array
                                            value = [file, subdir, card, str(hdr1[card]), str(hdr1.comments[card]),'','X']
                                            #print(value)
                                            values.append(value)

                                            #MySQL Inserts
                                            #sql += "INSERT INTO fits_catalog (Filename, Directory, Attribute, Value, Comments, Hdr0, Hdr1) VALUES ('"+file+"', '"+subdir+"', '"+card+"', '"+str(hdr1[card])+"', '"+str(hdr1.comments[card])+"', 'X', '');"
                                            mysql_inserts_vals(value)
                                        except VerifyError:
                                            if card == "OBSERVER":
                                                value = [file, subdir, card, "Invalid Characters", "Invalid Characters",'','X']
                                                print(value)

                                                value.append(value)

                                                #MySQL Inserts
                                                #sql += "INSERT INTO fits_catalog (Filename, Directory, Attribute, Value, Comments, Hdr0, Hdr1) VALUES ('"+file+"', '"+subdir+"', '"+card+"', '"+str(hdr1[card])+"', '"+str(hdr1.comments[card])+"', 'X', '');"
                                                mysql_inserts_vals(value)
                                            pass
                                    break
                                except OSError:
                                    #print(file + " OSError")
                                    pass
                                except IndexError:
                                    #print(file + " Index Error")
                                    pass
                                except ValueError:
                                    print("Value Error")
                                    # Retry fixing header
                                    pass
                                except VerifyError:
                                    print("Verify Error")
                                    # Retry fixing header
                                    pass

                            #print(values)

                            # Append CSV values
                            dataFrame = pd.DataFrame(values, columns = columns)
                            dataFrame.to_csv (csvdir + 'FITSScraperCSV' + '.csv', mode = 'a', index = False, header=True)
                            dataFrame = pd.DataFrame()

                            hdr = []
                            hdul.close()
                            #mysql_inserts_sql(sql)

                    else:
                        pass

            print("Process completed")


if __name__ == "__main__":
   main(sys.argv[1:])
