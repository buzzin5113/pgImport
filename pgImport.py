"""
pgImport - импоррт данных в СУБД PostgreSQL из формата CSV.
Включает в себя закачку данных с FTP, разархивирование, контроль скачанных и загруженных файлов

Timofeev Alexey, buzzin@mail.ru
"""

import csv
import sys
import os
from ftplib import FTP
import psycopg2
import zipfile
import smtplib


class Worker:
    def __init__(self, settings):
        self.set = __import__(settings)
        self.path_todownload = ".\\download\\"
        self.path_tounpack = ".\\unpack\\"
        self.conn = None
        self.cursor = None
        self.arcfilelist = []
        self.importfilenamelist = []
        self.importfilename = ""
        self.emailtxt = ""
        self.count = 0

    def db_connect(self):
        print('Connecting to the PostgreSQL database.')
        try:
            self.conn = psycopg2.connect(host=self.set.dbhost, port=self.set.dbport, database=self.set.dbname,
                                         user=self.set.dbuser, password=self.set.dbpasswd)
        except Exception as e:
            Worker.system_exit('db_connect', e)

    def db_disconnect(self):
        print('Disconnect database.')
        self.conn.close()

    def check_ftpfilename(self, filename):
        try:
            print("Check filename: {}".format(filename))
        #    arg = []
        #    arg.append(filename)

            self.cursor = self.conn.cursor()
            self.cursor.execute('select filename from db.system_lotus_ftp_file where filename=%s;', [filename, ])
            data = self.cursor.fetchone()
            if data:
                print("File already is imported")
            self.conn.commit()
            self.cursor.close()
            return data;
        except Exception as e:
            Worker.system_exit('check_ftpfilename', e)

    def ftp_load(self):
        """
        Загрузка файлов с FTP
        """
        try:
            ftp = FTP()
    #       ftp.set_debuglevel(1)
            ftp.connect(host=self.set.ftphost, port=self.set.ftpport, ignorepasvaddress=self.set.ftpignorepasvaddress)
            print("Connect to FTP - Ok")
            ftp.login(user=self.set.ftpuser, passwd=self.set.ftppasswd)
            print("Login on FTP - Ok")

            ftp.getwelcome()
            ftp.pwd()

            contents = ftp.nlst()
            for filename in contents:
                #Проверим что еще не скачивали этот файл
                if self.check_ftpfilename(filename):
                    continue
                print('Download file: {}'.format(filename))
                path = self.path_todownload + filename
                with open(path, "wb") as file:
                    ftp.retrbinary("RETR {}".format(filename), file.write)
                self.arcfilelist.append(filename)
            ftp.quit()

            if self.arcfilelist:
                self.file_unpack()
            else:
                print("No files to import.")

        except Exception as e:
            Worker.system_exit('ftp_load', e)

    def file_unpack(self):
        try:
            print('Listing files for unpack and import:\n')
            for file in self.arcfilelist:
                print('\t' + file)
            print('\t')
            for file in self.arcfilelist:
                zf = zipfile.ZipFile(self.path_todownload + file)
                self.importfilenamelist = zf.namelist()
                for filename in self.importfilenamelist:
                    print("File to unpack: " + filename)
                    zf.extract(filename, self.path_tounpack)
                    print("Unpack - Ok")
                    self.file_import(self.path_tounpack + filename)

                zf.close()
                self.cursor = self.conn.cursor()
                data = [file,]
                self.cursor.execute("insert into db.system_lotus_ftp_file values (%s)", data)
                self.conn.commit()
                self.cursor.close()
        except Exception as e:
            Worker.system_exit('file_unpack', e)

    def file_import(self, filepath):
        try:
            print('Import file {}'.format(filepath))
            self.count = 0
            self.cursor = self.conn.cursor()
            with open(filepath, encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file, delimiter=';')
                filename = os.path.basename(filepath)

                next(reader, None)
                for row in reader:
                    for i in range(0, 61):
                        if row[i] == 'NULL':
                            row[i] = None
                    row.append(filename)
                    row.append('now()')
                    self.cursor.execute("insert into db.lotus values ("
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s)", row)
                    if self.count % 100 == 0:
                        print(str(self.count) + '\r', end='')
                    self.count += 1

            self.conn.commit()
            self.cursor.close()
            print("Commit - Ok!")
            print('Import - Ok')
        except Exception as e:
            Worker.system_exit('file_unpack', e)

    def email_send(self, text):
        try:
            self.text = text
            body = "\r\n".join((
                                "From: %s" % self.set.emailfrom,
                                "To: %s" % self.set.emailto,
                                "Subject: %s" % self.set.emailsubject,
                                "",
                                self.text
                                ))
            server = smtplib.SMTP(self.set.emailhost)
        #    server.set_debuglevel(1)
            server.starttls()
            server.login(self.set.emaillogin, self.set.emailpasswd)
            server.sendmail(self.set.emailfrom, self.set.emailto, body)
            server.quit()
        except Exception as e:
            print("Error send email")
            print(e)
            sys.exit(-1)

    @staticmethod
    def system_exit(method, error):
        if method == 'db_connect':
            print('Error connect to database. Exit to system.')
            print(error)
            sys.exit(1)
        elif method == 'ftp_load':
            print('Error in module FTP. Exit to system')
            print(error)
            sys.exit(2)
        elif method == 'check':
            print('Error in module check_importfilename. Exit to system')
            print(error)
            sys.exit(3)
        elif method == 'check':
            print('Error in module file_unpack. Exit to system')
            print(error)
            sys.exit(4)
        elif method == 'check':
            print('Error in module file_import. Exit to system')
            print(error)
            sys.exit(5)
        else:
            print('Unexpected error')
            print(error)
            sys.exit(-1)


def main():
    print("pgImport starting.")

    work = Worker('settinglotus')
    work.db_connect()
    work.ftp_load()
    work.db_disconnect()

    work.email_send("Import is Ok")
    print('Successful. Exit to system.')
    sys.exit(0)


if __name__ == "__main__":
    main()
