import pickle
import h5py
import os
import sys
import requests
import tarfile
import locale


def open_pk(pk_path):
    with open(pk_path, 'rb') as f:
        pk = pickle.load(f)
    return pk


def open_h5(h5_path):
    if sys.platform == "win32":
        lo = locale.getlocale(locale.LC_ALL)[1]
        if lo and lo.lower() == "utf-8":
            h5_path = h5_path.encode("utf-8")
    f = h5py.File(h5_path, 'r')
    return f


def download(url, save_path):  # 中途有可能断连，能够正常解压，但读取时会出问题，提示被截断了
    _, file_name = os.path.split(url)
    file_path = os.path.join(save_path, file_name)
    r = requests.get(url, stream=True)
    total_size = int(r.headers.get('content-length'))
    temp_size = 0
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                temp_size += len(chunk)
                done = int(50 * temp_size / total_size)
                sys.stdout.write("\r[%s%s] %d%%" % ('█' * done, ' ' * (50 - done), 100 * temp_size / total_size))
                sys.stdout.flush()
        print('\nsuccessfully download {}'.format(file_name))
    return file_path


def extract(tar_path, target_path, tar_type):
    type_dict = {'gz': 'r:gz', 'bz2': 'r:bz2'}
    tar = tarfile.open(tar_path, type_dict[tar_type])
    file_names = tar.getnames()
    for file_name in file_names:
        tar.extract(file_name, target_path)
    tar.close()
    tar_name = os.path.basename(tar_path)
    print('successfully extract {}'.format(tar_name))


def convert_date_to_int(dt):
    t = dt.year * 10000 + dt.month * 100 + dt.day
    t *= 1000000
    return t

