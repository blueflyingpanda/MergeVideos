import shutil

import requests
import subprocess
from tqdm import tqdm
from pathlib import Path
from typing import Generator


PARTS_DIR = 'video_parts'
VIDEOS_DIR = 'downloaded_videos'
PARTS_FILE = 'ts_files.txt'


def map_to_url(from_file: str) -> dict[str, str]:

    with open(from_file) as fr:
        lines = tuple((line.strip() for line in fr.readlines()))

    return dict(zip(lines[::2], lines[1::2]))


def get_ts_url(url: str, base_url) -> str | None:

    if url.startswith('.'):
        return f'{base_url}{url[1:]}'  # remove leading '.'

    return url if url.startswith('https://') else None


def extract_ts_urls(from_url: str) -> Generator[str, None, None]:
    response = requests.get(from_url)
    base_url, _, _ = from_url.rpartition('/')

    return (ts_url.strip() for line in response.text.split('\n') if (ts_url := get_ts_url(line, base_url)) is not None)


def download_video_parts(to_directory: str, urls: Generator[str, None, None]) -> int:
    """

    :param to_directory: where to save
    :param urls: from where to download
    :return: amount of files
    """
    i = 0

    for url in tqdm(urls):
        response = requests.get(url, timeout=60)

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
    print('Parse config file.')

    video_to_url = map_to_url(from_file='video_urls.txt')

    print(f'{len(video_to_url)} videos specified in config.')

    Path(VIDEOS_DIR).mkdir(exist_ok=True)

    for name, url in video_to_url.items():
        print(f'Get video: {name}')

        Path(PARTS_DIR).mkdir(exist_ok=True)

        urls = extract_ts_urls(from_url=url)
        number_of_parts = download_video_parts(to_directory=PARTS_DIR, urls=urls)
        list_parts(in_file=PARTS_FILE, number_of_parts=number_of_parts)

        print(f'Merge {number_of_parts} video parts.')
        subprocess.run(['ffmpeg', '-f', 'concat', '-i', PARTS_FILE, '-c', 'copy', f'{VIDEOS_DIR}/{name}.mp4'])

        shutil.rmtree(PARTS_DIR)
