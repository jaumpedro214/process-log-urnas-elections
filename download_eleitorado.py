import os

URL = 'https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_2022.zip'
PATH = os.path.join('data', 'perfil_eleitorado_2022.zip')

print(f'Iniciando download de {URL}')
os.system(f'wget -O {PATH} {URL}')
print(f'Finalizado download de {URL}')

# unzip file
os.system(f'unzip -o {PATH} -d {PATH[:-4]}')
# Remove zip
os.system(f'rm {PATH}')
