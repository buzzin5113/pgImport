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
        self.path_todownload = "./download/"
        self.path_tounpack = "./unpack/"
        self.conn = None
        self.cursor = None
        self.arcfilelist = []
        self.importfilenamelist = []
        self.importfilename = ""
        self.emailtxt = 'pgImport started.\r\n'
        self.count = 0

    def db_connect(self):
        print('Connecting to the PostgreSQL database.')
        try:
            self.conn = psycopg2.connect(host=self.set.dbhost, port=self.set.dbport, database=self.set.dbname,
                                         user=self.set.dbuser, password=self.set.dbpasswd)
        except Exception as e:
            Worker.system_exit(self, 'db_connect', e)

    def db_disconnect(self):
        print('Disconnect database.')
        self.conn.close()

    def check_ftpfilename(self, filename):
        try:
            print("Check filename: {}".format(filename))
            self.cursor = self.conn.cursor()
            self.cursor.execute('select filename from {0} where filename=%s;'.format(self.set.ftplistfilestable), [filename, ])
            data = self.cursor.fetchone()
            if data:
                print("File already is imported")
            self.conn.commit()
            self.cursor.close()
            return data;
        except Exception as e:
            Worker.system_exit(self, 'check_ftpfilename', e)

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

            # Получаем список файлов с FTP сервера
            contents = ftp.nlst()
            # В цикле обрабатываем все имена
            for filename in contents:
                # Проверим маску файла на допустимые, берем начало строки до символа '-' или '_'
                if filename.replace('_', '-').split('-')[0] not in self.set.ftpmaskfile:
                    print('Wrong filename mask {0} - skip file'.format(filename))
                    self.emailtxt += 'Wrong filename mask - skip file {} \n\n'.format(filename)
                    # Если маски нет в списке, то пропускаем и начинаем цикл загрузки заново
                    continue
                # Проверим что еще не скачивали этот файл
                if self.check_ftpfilename(filename):
                    continue
                print('Download file: {}'.format(filename))
                path = self.path_todownload + filename
                with open(path, "wb") as file:
                    ftp.retrbinary("RETR {}".format(filename), file.write)
                self.emailtxt = self.emailtxt + 'Download new file "' + filename + '" - Ok!\n'
                self.arcfilelist.append(filename)
            ftp.quit()

            if self.arcfilelist:
                self.file_unpack()
            else:
                print("No files to import.")

        except Exception as e:
            Worker.system_exit(self, 'ftp_load', e)

    def file_unpack(self):
        try:
            print('Listing files for unpack and import:\n')
            for file in self.arcfilelist:
                print('\t' + file)
            print('\t')

            self.cursor = self.conn.cursor()

            for file in self.arcfilelist:
                self.emailtxt = self.emailtxt + '   Processing archive - "' + file + '"\n'
                zf = zipfile.ZipFile(self.path_todownload + file)
                self.importfilenamelist = zf.namelist()
                for filename in self.importfilenamelist:
                    print("File to unpack: " + filename)
                    zf.extract(filename, self.path_tounpack)
                    print("Unpack - Ok")
                    self.emailtxt = self.emailtxt + '        Unpack file: ' + filename + ' - Ok.\n'
                    self.file_import(self.path_tounpack + filename)
                zf.close()

                self.cursor.execute("insert into {0} values (%s)".format(self.set.ftplistfilestable), [file, ])
                self.conn.commit()
                self.cursor.close()

        except Exception as e:
            Worker.system_exit(self, 'file_unpack', e)

    def file_import(self, filepath):
        try:
            self.count = 0
            filename = os.path.basename(filepath)
            print('Import file {}'.format(filepath))
            self.emailtxt += '        Import file {} \n'.format(filename)
            tableindex = filename.replace('_', '-').split('-')

            if self.set.tabledict.get(tableindex[0]):
                tablename = self.set.tabledict.get(tableindex[0])
            else:
                print("        Not found table in table dic.")
                self.emailtxt += '        Not found table in table dic\n'
                self.system_exit('unknown table')

            with open(filepath, encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file, delimiter=';')

                next(reader, None)
                for row in reader:
                    for i in range(0, 61):
                        if row[i] == 'NULL':
                            row[i] = None
                    row.append(filename)
                    row.append('now()')
                    self.cursor.execute("insert into {0} values ("
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s)".format(tablename), row)
                    if self.count % 100 == 0:
                        print(str(self.count) + '\r', end='')
                    self.count += 1
                print('Total rows: ', self.count)

            self.emailtxt = self.emailtxt + '            Import file : "' + filename + '" - Ok. Row added - ' + \
                            str(self.count) + '\n'
            print("Commit - Ok!")
            print('Import - Ok')
        except Exception as e:
            Worker.system_exit(self, 'file_import', e)

    def email_send(self):
        #print(self.emailtxt)
        try:
            body = u"\r\n".join((
                                "From: %s" % self.set.emailfrom,
                                "To: %s" % self.set.emailto,
                                "Subject: %s" % self.set.emailsubject,
                                "",
                                self.emailtxt,
                                'Successful.'
                                )).encode('utf-8')
            #print(body)
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

    def system_exit(self, method, error=None):
        self.set.emailsubject = self.set.emailsubjecterror
        if method == 'db_connect':
            print('Error connect to database. Exit to system.')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Error connect to database. Exit to system.\n'
         #   self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(1)
        elif method == 'ftp_load':
            print('Error in module FTP. Exit to system')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Error in module FTP. Exit to system.\n'
        #    self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(2)
        elif method == 'check_ftpfilename':
            print('Error in module check_importfilename. Exit to system')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Error in module check_importfilename. Exit to system.\n'
        #    self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(3)
        elif method == 'file_unpack':
            print('Error in module file_unpack. Exit to system')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Error in module file_import. Exit to system.\n'
        #    self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(4)
        elif method == 'file_import':
            print('Error in module file_import. Exit to system')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Error in module file_import. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(5)
        elif method == 'unknown table':
            print('Unknown table. Exit to system')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Unknown table. Exit to system.\n'
            self.email_send()
            sys.exit(5)
        else:
            print('Unexpected error')
            print(error)
            self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
            self.emailtxt += 'Unexpected error.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(-1)


def main():
    print("pgImport starting.")

    work = Worker('settinglotus')
    work.db_connect()
    work.ftp_load()
    work.db_disconnect()

    work.email_send()
    print('Successful. Exit to system.')
    sys.exit(0)


if __name__ == "__main__":
    main()
