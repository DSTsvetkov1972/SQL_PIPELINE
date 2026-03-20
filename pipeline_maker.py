from tkinter import filedialog
import os


selected_files = filedialog.askopenfilenames(title="Выберите несколько файлов с SQL-конвеером!")
project = 'Проверка жд тарифа с вагонами.sql'
dir = os.path.dirname(selected_files[0])

pipeline_dir = os.path.join(dir, 'pipeline')

pipeline_file = os.path.join(pipeline_dir, project)

files_list = []
for file in selected_files:

    with open(file, encoding='utf-8') as f:
        files_list.append(f"--{ file }\n")
        files_list.append(f.read())

# print(files_list)

pipeline = '\n'.join(files_list)


if not os.path.exists(pipeline_dir):
    os.mkdir(pipeline_dir)

with open(pipeline_file, encoding='utf-8', mode='w') as f:
    f.write(pipeline)

print('Конвеер создан')
