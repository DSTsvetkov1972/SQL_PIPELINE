from fns import pipeline, get_sql_file
from colorama import Fore, init, Style
from logo import logo_colored
import os
from fns import flicker

init()
print(Style.BRIGHT)
print(logo_colored)


choise = "0"


while True:

    if choise == "0":
        print(Fore.YELLOW + " Нажмите ввод, чтобы выбрать файл конвеера!")
        input()
        sql_source = get_sql_file()   
    if choise == "2": 
        sql_source = get_sql_file()   
    
    sql_file_name = sql_source["sql_file_name"]

    pipe_results_file = sql_source["pipe_results_file"]

    if os.path.exists(pipe_results_file):
        with open(pipe_results_file, 'r', encoding = 'utf-8') as f:
            results = f.read()
            if results:
                flicker(" Этот конвеер уже запускался!", pause = 0.5, finish_Fore=Fore.RED)
                print(Fore.YELLOW + " Успешно выполненные шаги:")
                print(Fore.GREEN + results + Fore.RESET)

                while True:
                    print(Fore.YELLOW + " Выберите:\n" +
                          Fore.WHITE + " 1" + Fore.CYAN + " - чтобы запустить с места останова\n" +
                          Fore.WHITE + " 2" + Fore.CYAN + " - чтобы запустить конвеер сначала" + Fore.RESET)
                    choise = input(" Вы выбрали: ")
                    if choise == "2":
                        open(pipe_results_file, 'w', encoding = 'utf-8')
                        break
                    if choise == "1":
                        break

    else:
        open(pipe_results_file, 'w', encoding = 'utf-8')

    pipeline(sql_file_name, pipe_results_file)                       
    
    print(logo_colored)
    print(Fore.YELLOW + " Выберите:\n" +
          Fore.WHITE + " 1" + Fore.CYAN + " - чтобы запустить тот же конвеер\n" +
          Fore.WHITE + " 2" + Fore.CYAN + " - чтобы запустить новый конвеер" + Fore.RESET)

    choise = input(" Вы выбрали: ")
    