"""
pgImport - импоррт данных в СУБД PostgreSQL из формата CSV.
Включает в себя закачку данных с FTP, разархивирование, контроль скачанных и загруженных файлов

Timofeev Alexey, buzzin@mail.ru
"""

import csv
import sys
from ftplib import FTP
import psycopg2


class Worker:
    def __init__(self, settings):
        self.set = __import__(settings)
        self.set.path_todownload = ".\\download\\"
        self.conn = None
        self.cursor = None
        self.filelist = []

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
            print("Check filename")

            data = None;
            arg = []
            arg.append(filename)

            self.cursor = self.conn.cursor()
            self.cursor.execute('select filename from db.system_lotus_ftp_file where filename=%s;', arg)
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
                path = self.set.path_todownload + filename
                with open(path, "wb") as file:
                    ftp.retrbinary("RETR {}".format(filename), file.write)
                self.filelist.append(filename)
            ftp.quit()

            self.file_unpack()

        except Exception as e:
            Worker.system_exit('ftp_load', e)

    def file_unpack(self):
        print('Listing files for unpack and import:\n')
        for file in self.filelist:
            print('\t' + file)
        print('\t')
        for file in self.filelist:
            self.file_import(file)

    def file_import(self, filename):
        print('Import file {}'.format(filename))
        print('Import - Ok')

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

    print('Successful. Exit to system.')
    sys.exit(0)


if __name__ == "__main__":
    main()
