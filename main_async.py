import asyncio
import aiohttp
import shutil
import subprocess
from tqdm import tqdm
from pathlib import Path

PARTS_DIR = 'video_parts'
VIDEOS_DIR = 'downloaded_videos'
PARTS_FILE = 'ts_files.txt'


def map_to_url(from_file: str) -> dict[str, str]:
    with open(from_file) as fr:
        lines = tuple(line.strip() for line in fr)
    return dict(zip(lines[::2], lines[1::2]))


def get_ts_url(url: str, base_url: str) -> str | None:
    if url.startswith('.'):
        return f'{base_url}{url[1:]}'
    return url if url.startswith('https://') else None


def extract_ts_urls(from_text: str, base_url: str) -> list[str]:
    return [
        ts_url.strip()
        for line in from_text.split('\n')
        if (ts_url := get_ts_url(line, base_url)) is not None
    ]


async def fetch(session: aiohttp.ClientSession, url: str, dest: Path) -> None:
    async with session.get(url, timeout=60) as resp:
        content = await resp.read()
        dest.write_bytes(content)


async def download_video_parts(to_directory: str, urls: list[str]) -> int:
    Path(to_directory).mkdir(exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, url in enumerate(urls):
            dest = Path(to_directory) / f'{i}.ts'
            tasks.append(fetch(session, url, dest))

        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await f

    return len(urls)


def list_parts(in_file: str, number_of_parts: int) -> None:
    with open(in_file, 'w') as fw:
        for n in range(number_of_parts):
            fw.write(f"file '{PARTS_DIR}/{n}.ts'\n")


async def main():
    print('Parse config file.')
    video_to_url = map_to_url('video_urls.txt')
    print(f'{len(video_to_url)} videos specified in config.')

    Path(VIDEOS_DIR).mkdir(exist_ok=True)

    async with aiohttp.ClientSession() as session:
        for name, url in video_to_url.items():
            print(f'Get video: {name}')
            Path(PARTS_DIR).mkdir(exist_ok=True)

            response = await session.get(url)
            text = await response.text()
            base_url, _, _ = url.rpartition('/')
            urls = extract_ts_urls(text, base_url)

            number_of_parts = await download_video_parts(PARTS_DIR, urls)
            list_parts(PARTS_FILE, number_of_parts)

            print(f'Merge {number_of_parts} video parts.')
            subprocess.run(['ffmpeg', '-f', 'concat', '-i', PARTS_FILE, '-c', 'copy', f'{VIDEOS_DIR}/{name}.mp4'])

            shutil.rmtree(PARTS_DIR)


if __name__ == '__main__':
    asyncio.run(main())
