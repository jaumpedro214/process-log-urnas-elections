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

NUM_TRHEADS = 1

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

    if turnos_uf_queue.empty():
        print('All downloads finished')
    else:
        print(f'{turnos_uf_queue.qsize()} downloads remaining')
        download_file()

    return
    


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
