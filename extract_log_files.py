import os

BASE_PATH = './data/logs'

def unzip_log_files( path ):
    # unzip file
    os.system(f'7z e {path} -o{path[:-4]} *.logjez -r')

    # Remove unnecessary files
    os.system(f'rm {path}') # Zip file
    # os.system(f'rm {path[:-4]}/*.rdv')
    # os.system(f'rm {path[:-4]}/*.vscmr')
    # os.system(f'rm {path[:-4]}/*.imgbu')
    # os.system(f'rm {path[:-4]}/*.bu')

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
                f'7z e {path[:-4]}/{file} -y -o{path[:-4]}/{filename} > /dev/null'
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
    for file in os.listdir(BASE_PATH):
        if file.endswith('.zip'):
            unzip_log_files(os.path.join(BASE_PATH, file))