import threading
import queue
import os
from itertools import product

BASE_URL = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2022_{}t_{}.zip'
UFS_BR = [
    'AC', 'AL', 'AP', 'AM', 
    'BA', 'CE', 'DF', 'ES', 
    'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 
    'PE', 'PI', 'RJ', 'RN', 
    'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO', 'ZZ'
]
TURNOS = [1, 2]
NUM_TRHEADS = 4

# Create a queue to communicate with the worker threads
turnos_uf_queue = queue.Queue()

def download_file():

    uf_turno = turnos_uf_queue.get()
    url = BASE_URL.format(*uf_turno)
    path = os.path.join('data', 'logs', f'{uf_turno[0]}_{uf_turno[1]}.zip')

    print(f'Downloading {url} to {path}')

    print(f'Iniciando download de {url}')
    os.system(f'wget -O {path} {url}')
    print(f'Finalizado download de {url}')

    # unzip file
    os.system(f'unzip -o {path} -d {path[:-4]}')

    # Remove unnecessary files
    os.system(f'rm {path}') # Zip file
    os.system(f'rm {path[:-4]}/*.rdv')
    os.system(f'rm {path[:-4]}/*.vscmr')
    os.system(f'rm {path[:-4]}/*.imgbu')
    os.system(f'rm {path[:-4]}/*.bu')


    # list all files in the directory
    files = os.listdir(path[:-4])

    # Iterate over the list of files
    # and extract the .logjez files
    for file in files:
        if file.endswith('.logjez'):
            # Extract the .logjez file
            # and rename it to .csv
            filename=file[:-7]
            os.system(
                f'7z e {path[:-4]}/{file} -y -o{path[:-4]}/{filename}'
            )
            os.system(
                f'mv {path[:-4]}/{filename}/logd.dat {path[:-4]}/{filename}.csv'
            )
            os.system(
                f'rm -r {path[:-4]}/{filename}'
            )

    os.system(f'chmod 777 -R {path[:-4]}')
    os.system(f'rm {path[:-4]}/*.logjez')


if __name__ == "__main__":

    for uf_br, turno in product(UFS_BR, TURNOS):
        turnos_uf_queue.put((turno, uf_br))

    for i in range(NUM_TRHEADS):
        worker = threading.Thread(
            target=download_file,
            daemon=True
        )
        worker.start()

    turnos_uf_queue.join()

    print("Done")
