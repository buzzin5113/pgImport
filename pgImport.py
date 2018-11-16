"""
pgImport - импоррт данных в СУБД PostgreSQL из формата CSV.
Включает в себя закачку данных с FTP, разархивирование, контроль скачанных и загруженных файлов

Краткий алгоритм работы:
1. Создание экземпляра класса Worker с импортом данных из файла настроек
2. Коннект к FTP серверу и получение списка файлов(архивов) в корневой директрии
3. Проверка на наличие новых файлов, список уже закачанных архивов берется из БД
4. Проверка на совпадение с известной маской файла
5. Скачивание новых файлов
6. Распаковка кождого файла (предполагается использование архиватора ZIP)
7. Проверка имени распакованного файла на совпадение с известной маской для определения таблицы для импорта.
    Если маска не найдена, то пропуск файла
8. После окончания импорта всех файлов из таблицы - обновление таблицы со списком импортированных архивов

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
    """
    Содержит весь функционал программы
    """
    def __init__(self, settings):
        # Импорт настроек из внешнего .py файла
        self.set = __import__(settings)
        # Перечень переменных
        self.path_todownload = "./download/"
        self.path_tounpack = "./unpack/"
        self.conn = None
        self.cursor = None
        self.arcfilelist = []
        self.importfilenamelist = []
        self.importfilename = ""
        self.emailtxt = 'pgImport started.\r\n'
        self.count = 0
        self.tablename = ''
        self.truncate = ''
        self.delimiter = ''
        self.encoding = ''
        self.sqltext = ''

    def db_connect(self):
        """
        Подключение к БД.
        Параметры подключения устанавливаются в конструкторе класса
        """
        print('Connecting to the PostgreSQL database.')
        try:
            self.conn = psycopg2.connect(host=self.set.dbhost, port=self.set.dbport, database=self.set.dbname,
                                         user=self.set.dbuser, password=self.set.dbpasswd)
        except Exception as e:
            Worker.system_exit(self, 'db_connect', e)

    def db_disconnect(self):
        """
        Отключение от БД
        """
        print('Disconnect database.')
        self.conn.close()

    def check_ftpfilename(self, filename):
        """
        Проверка на
        :param filename: - имя архива на FTP
        :return:
        """
        try:
            print("Check filename: {}".format(filename))
            self.cursor = self.conn.cursor()
            self.cursor.execute('select filename from {0} where filename=%s;'.format(self.set.ftplistfilestable),
                                [filename, ])
            data = self.cursor.fetchone()
            if data:
                print("File already is imported")
            self.conn.commit()
            self.cursor.close()
            return data
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

            # Запускаем процедуру разархивирования для каждого скачанного файла
            # Там же происходит и импорт файлов в БД
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

            for file in self.arcfilelist:
                self.cursor = self.conn.cursor()
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
                print("    Commit - Ok!")
                print("    Import - Ok!")
                self.emailtxt += '    Commit import archive "{0}" - Ok!\n'.format(file)

        except Exception as e:
            Worker.system_exit(self, 'file_unpack', e)

    def file_import(self, filepath):
        try:
            self.count = 0
            filename = os.path.basename(filepath)
            print('Import file {}'.format(filepath))
            self.emailtxt += '        Import file {} \n'.format(filename)
            tableindex = filename.replace('_', '-').replace('.', '-').split('-')

            # Проверка на сопоставление имени файла и имени таблицы для импорта
            if self.set.tabledict.get(tableindex[0]):

                # Загрузка настроек импорта из словаря в настройках
                self.tablename = self.set.tabledict.get(tableindex[0]).get('tablename')
                self.truncate = self.set.tabledict.get(tableindex[0]).get('truncate')
                self.delimiter = self.set.tabledict.get(tableindex[0]).get('delimiter')
                self.encoding = self.set.tabledict.get(tableindex[0]).get('encoding')

                # Проверяем на необходимость очистки таблицы
                if self.truncate:
                    self.sqltext = 'truncate table {0};'.format(self.tablename)
                    self.cursor.execute(self.sqltext)
                    print('        Trancate table {} - Ok!'.format(self.tablename))
                    self.emailtxt += '        Trancate table {} - Ok!\n'.format(self.tablename)

                # Определим количество столбцов в таблице назначения
                self.sqltext = "select count(*) from information_schema.columns " \
                               "where table_schema = '{0}' and table_name = '{1}';".format(self.tablename.split('.')[0],
                                                                                           self.tablename.split('.')[1])
                self.cursor.execute(self.sqltext);
                column_count = self.cursor.fetchone()[0]

                # Формируем SQL запрос на вставку
                s = "%s," * column_count
                self.sqltext = "insert into {0} values (" + s
                self.sqltext = self.sqltext[:-1]
                self.sqltext += ");"
                self.sqltext = self.sqltext.format(self.tablename)

                # Открываем CSV файл
                with open(filepath, encoding=self.encoding) as csv_file:
                    reader = csv.reader(csv_file, delimiter=self.delimiter)

                    # Пропускаем заголовок файла
                    next(reader, None)

                    # Обрабатываем по одной строке
                    for row in reader:

                        # Проходим все столбцы и обрабатываем NULL
                        for i in range(0, column_count - 2):
                            if row[i] == 'NULL' or row[i] == '':
                                row[i] = None

                        # Добавляем данные для стандартных колонок filename и date_import
                        row.append(filename)
                        row.append('now()')

                        # Выполняем запрос на вставку
                        self.cursor.execute(self.sqltext, row)

                        # Печать счетчика вставленных строк через каждые 100 строк
                        if self.count % 100 == 0:
                            print(str(self.count) + '\r', end='')

                        self.count += 1

                    print('Total rows: ', self.count)

                self.emailtxt = self.emailtxt + '                Import file : "' + filename + \
                                '" - Ok. Row added - ' + str(self.count) + '\n'
            else:
                print("            Not found table in table dic.")
                self.emailtxt += '            Not found table in table dic\n'

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
        self.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
        if method == 'db_connect':
            print('Error connect to database. Exit to system.')
            print(error)
            self.emailtxt += 'Error connect to database. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(1)
        elif method == 'ftp_load':
            print('Error in module FTP. Exit to system')
            print(error)
            self.emailtxt += 'Error in module FTP. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(2)
        elif method == 'check_ftpfilename':
            print('Error in module check_importfilename. Exit to system')
            print(error)
            self.emailtxt += 'Error in module check_importfilename. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(3)
        elif method == 'file_unpack':
            print('Error in module file_unpack. Exit to system')
            print(error)
            self.emailtxt += 'Error in module file_import. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(4)
        elif method == 'file_import':
            print('Error in module file_import. Exit to system')
            print(error)
            self.emailtxt += 'Error in module file_import. Exit to system.\n'
            self.emailtxt += str(error) + '\n'
            self.email_send()
            sys.exit(5)
        elif method == 'unknown table':
            print('Unknown table. Exit to system')
            print(error)
            self.emailtxt += 'Unknown table. Exit to system.\n'
            self.email_send()
            sys.exit(5)
        else:
            print('Unexpected error')
            print(error)
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
