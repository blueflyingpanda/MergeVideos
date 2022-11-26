import requests
import subprocess
from tqdm import tqdm
from pathlib import Path
from typing import Generator


PARTS_DIR = 'video_parts'
PARTS_FILE = 'ts_files.txt'
Path(PARTS_DIR).mkdir(exist_ok=True)


def get_urls(from_file: str) -> Generator[str, None, None]:
    with open(from_file) as fr:
        text = fr.readlines()
    return (line.strip() for line in text if line.startswith('https://'))


def download_video_parts(to_directory: str, urls: Generator[str, None, None]) -> int:
    """

    :param to_directory: where to save
    :param urls: from where to download
    :return: amount of files
    """
    i = 0
    for url in tqdm(urls):
        response = requests.get(url)
        with open(f'{to_directory}/{i}.ts', 'wb') as fwb:
            fwb.write(response.content)
        i += 1
    return i


def list_parts(in_file: str, number_of_parts: int) -> None:
    with open(in_file, 'w') as fw:
        n = 0
        while n < number_of_parts:
            fw.write(f"file '{PARTS_DIR}/{n}.ts'\n")
            n += 1


if __name__ == '__main__':

    urls = get_urls(from_file='video_urls.txt')
    number_of_parts = download_video_parts(to_directory=PARTS_DIR, urls=urls)
    list_parts(in_file=PARTS_FILE, number_of_parts=number_of_parts)

    subprocess.run(['ffmpeg', '-f', 'concat', '-i', PARTS_FILE, '-c', 'copy', 'video.mp4'])
