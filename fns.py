import dotenv
from clickhouse_driver import Client
import os
from colorama import Fore
from datetime import datetime
from tkinter import filedialog
from time import sleep
from threading import Thread

dotenv.load_dotenv(r"C:\Users\tsvetkovds\Documents\.Полезности\PYTHON\.env")

CLICK_HOST = os.getenv("CLICK_HOST")
CLICK_PORT = os.getenv("CLICK_PORT")
CLICK_DBNAME = os.getenv("CLICK_DBNAME")
CLICK_USER = os.getenv("CLICK_USER")
CLICK_PWD = os.getenv("CLICK_PWD")

# списки служат для передачи значений между потоками
# sql_exec - в этом потоке выполняется SQL-запрос формирующий таблицу
# spinner - крутилка в терминале, пока выполняется sql_exec
spinner_on = [None]      # 
exec_block = [None]      # если ошибка TimeOut Error в запросе exec_block = [True] еще раз запустить выполнение блока
break_pipe = [None]      # если ошибка в запросе break_pipe = [False] прервать выполнение конвеера
sql_block_state = [None] # значение, которое выводитися в конце работы spinner, может быть 'Ok' или 'Time Out Error' 


connection=Client(
    host=CLICK_HOST,
    port = CLICK_PORT,
    database = CLICK_DBNAME,
    user = CLICK_USER,
    password = CLICK_PWD,
    secure = True,
    verify=False)

def flicker(your_str, interval=0.05, repeats=7, fleaker_Fore=Fore.RED, finish_Fore=Fore.YELLOW, pause = 0):
    print(fleaker_Fore)
    for i in range(0, repeats):
        print('\033[F' + " " * len(your_str))
        sleep(interval)
        print('\033[F' + fleaker_Fore + your_str)
        sleep(interval)

    print('\033[F' + finish_Fore + your_str + Fore.RESET)
    sleep(pause)

def pprint_file_name(sql_file_name, stage=" Файл конвеера:\n"):

    path_parts = sql_file_name.split(r"/")
    folder = os.path.join('C:\\',*path_parts[1:-1])
    short_name = '.'.join(path_parts[-1].split(".")[:-1])
    file_extension = path_parts[-1].split(".")[-1]
    print(Fore.MAGENTA + stage +
          Fore.CYAN + f" {folder}\\" +
          Fore.WHITE + f"{short_name}.{file_extension}" +
          Fore.RESET) 

def get_pipe_results_file_name(sql_file_name):
    
    path_parts = sql_file_name.split(r"/")
    folder = os.path.join('C:\\',*path_parts[1:-1])
    short_name = '.'.join(path_parts[-1].split(".")[:-1])
    
    return os.path.join(folder, f"{short_name}.sql_pipe")




def get_sql_file():
    
    while True:
        # print(Fore.YELLOW + " Выберите файл с SQL-конвеером!" + Fore.RESET)

        selected_file = filedialog.askopenfile(title="Выберите файл с SQL-конвеером!")

   
        if selected_file:
            sql_file_name = selected_file.name

            pprint_file_name(sql_file_name)

            if sql_file_name.split(".")[-1].lower() != 'sql':
                flicker(" Выбранный файл должен иметь расширение .sql!", finish_Fore=Fore.RED)
                continue

            # Проверяем, чтобы внутри файла были блоки конвеера
            with open (sql_file_name, encoding='utf-8') as f:
                sql = f.read()
            if  "CREATE OR REPLACE TABLE " not in sql:       
                flicker(" Выбранный файл не содержит CREATE OR REPLACE TABLE!", finish_Fore=Fore.RED)
                continue


            
            pipe_results_file = get_pipe_results_file_name(sql_file_name)


            return {"sql_file_name": sql_file_name,
                    "pipe_results_file": pipe_results_file}
        else:
            flicker(" Не выбран файл с SQL-конвеером!", finish_Fore=Fore.RED)


def spinner(sql_block_first_line):
    cursors = [' \\', ' |', ' /', ' -']

    block_info = Fore.WHITE + f" {datetime.now()} " + Fore.BLUE + sql_block_first_line + Fore.RESET    

    while spinner_on[0]:
        for cursor in cursors:
            print('\033[F' + block_info + cursor)
            sleep(0.08)
    print('\033[F' + block_info + sql_block_state[0])


def sql_exec(sql_block, sql_block_first_line, pipe_results_file):

    spinner_on[0] = True
    sql_block_state[0] = ""  
    
    # Проверяем, был ли отработан блок

    with open (pipe_results_file, 'r') as f:
        res_str = f.read()
        if res_str:
            results = [result.split("\t")[1] for result in res_str.split('\n')[:-1]]
        else:
            results = []

    if sql_block_first_line in results:
        sql_block_state[0] = Fore.GREEN + ' успешно в предыдущем прогоне\n'
        exec_block[0] = False
        spinner_on[0] = False
        return

    try:
        with connection:
            connection.execute(sql_block)
            
            with open (pipe_results_file, 'a') as f:
                f.write(f" {datetime.now()}\t{sql_block_first_line}\n")

            sql_block_state[0] = Fore.GREEN + ' Ok\n'
            exec_block[0] = False
    except TimeoutError as e:
        sql_block_state[0] = Fore.RED + f' {e}\n' + Fore.RESET 
    except Exception as e:
        sql_block_state[0] = Fore.RED + f' {e}' + Fore.RESET
        exec_block[0]= False
        break_pipe[0] = True
    finally:
        spinner_on[0] = False


def pipeline(sql_file_name, pipe_results_file):
    
    with open(sql_file_name, encoding='utf-8') as f:
        sql = f.read()    

    sql = sql.split("--download")[0]

    block_start = 0
    block_starts = []


    while block_start!=-1:
        block_start = sql.find("CREATE OR REPLACE TABLE ", block_start + 1)

        block_starts.append(block_start)

    if block_starts != [-1]:
        pprint_file_name(sql_file_name+'\n', stage=" Запускаем конвеер:\n")

        for block_number, block_start in enumerate(block_starts[:-1]):

            block_finish =  block_starts[block_number+1]

            sql_block = sql[block_start: block_finish]
            sql_block_first_line = sql_block.split("\n")[0]

            
            break_pipe[0] = False
            exec_block[0] = True
            
            while exec_block[0]:

                sql_exec_thread = Thread(target=sql_exec, args=(sql_block, sql_block_first_line, pipe_results_file))           
                spinner_thread = Thread(target=spinner, args=(sql_block_first_line, ))

                sql_exec_thread.start()
                spinner_thread.start()
                
                sql_exec_thread.join()
                spinner_thread.join()


            if break_pipe[0]:
                return

        print(Fore.WHITE + f"\n { datetime.now() } " + Fore.GREEN + "Конвеер отработал!" + Fore.RESET)
    else:
        print(Fore.RED + " В выбранном файле нет ни одного запроса на создание таблицы!" + Fore.RESET)


if __name__ == '__main__':
    print(get_sql_file())