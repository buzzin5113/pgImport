"""
Settings for PostgreSQL connection
"""
dbhost = 'XXX.XXX.XXX.XXX'
dbport = 5432
dbname = 'XXXX'
dbuser = 'XXXX'
dbpasswd = 'XXXX'
"""
Import settings
"""
"""
tabledict - словарь описания параметров импорта таблицы
0 - имя таблицы
1 - необходимость truncate
2 - разделитель в CSV файле
3 - кодировка CSV файла
4 - сррипт перед началом импорта - НЕРЕАЛИЗОВАНО
5 - скрипт после окончания импорта
"""
tabledict = {'receipt': {'tablename': 'db.lotos', 'truncate': 0, 'delimiter': ';', 'encoding': 'utf-8',
                         'script_before': None, 'script_after': None},
             'products': {'tablename': 'db.lotos_products', 'truncate': 1, 'delimiter': '|', 'encoding': 'utf-8',
                          'script_before': None, 'script_after': None},
             'sigma': {'tablename': 'db.lotos_cost_buffer', 'truncate': 0, 'delimiter': '|', 'encoding': 'utf-8',
                          'script_before': None, 'script_after': 'select db.proc_transport_cost();'},
             'elite': {'tablename': 'db.lotos_cost_buffer', 'truncate': 0, 'delimiter': '|', 'encoding': 'utf-8',
                          'script_before': None, 'script_after': 'select db.proc_transport_cost();'},
             }
"""
commandlist - таблица содержащая список комманд для выполнения через pgExec
"""
commandlist = 'db.lotos_commands';
"""
Setting for FTP connection
"""
ftphost = 'XXX.XXX.XXX.XXX'
ftpport = 21
ftpignorepasvaddress = True
ftpuser = 'XXXX'
ftppasswd = 'XXXX'
ftpmaskfile = ['receipts', 'products', 'sigma', 'elite']
ftplistfilestable = 'db.lotos_system_ftp_file'
"""
Settings for email
"""
emailhost = 'XXXX.com:587'
emailsubject = 'TEST: import file'
emailsubjecterror = 'TEST: import file - ERROR'
emailto = 'XXXX@XXXX'
emailfrom = 'XXXX@XXXX'
emaillogin = 'XXXX'
emailpasswd = 'XXXX'
