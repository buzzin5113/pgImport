"""
Выполнение SQL команд

Тимофеев Алексей, buzzin@mail.ru
"""
import pgImport as im
import psycopg2


if __name__ == '__main__':
    commandlist = []

    Exec = im.Worker('settinglotus')
    Exec.set.emailtxt = 'pgExec started.\r\n\n'
    Exec.set.emailsubject = 'Exec commands'

    Exec.db_connect()
    Exec.cursor = Exec.conn.cursor()

    try:
        Exec.sqltext = 'select command from {0} order by id;'.format(Exec.set.commandlist)
        Exec.cursor.execute(Exec.sqltext)

        print('List commands:')
        for command in Exec.cursor:
            commandlist.append(command[0])
        Exec.conn.commit()
        print(commandlist)

        print('\nExecute commands')
        for command in commandlist:
            Exec.emailtxt += 'Execute command:\n{0}\n\n'.format(command)
            Exec.sqltext = command
            Exec.cursor.execute(Exec.sqltext)
            Exec.conn.commit()
            Exec.emailtxt += 'Execute - Ok.\n\n'

    except Exception as e:
        Exec.emailtxt += '!!!!!!!!!!!!!!!!!!!!!!!!\n'
        Exec.emailtxt += str(e)
        Exec.email_send()

    Exec.email_send()

    Exec.db_disconnect()
