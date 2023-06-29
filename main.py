import asyncio
import os
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List

from logger import get_logger
import config
import asyncssh


timestamp = datetime.now().strftime('%Y%m%d_%H%M%S%f')
logger = get_logger(filename=timestamp)
concurrency_limit = asyncio.Semaphore(config.batch_size)


@dataclass
class DownloadFileTask:
    sftp: asyncssh.SFTPClient
    file: str
    localdir: str
    idx: int
    total_count: int
    completed: bool = False

    async def execute(self):
        local_file_path = Path(f"{self.localdir}/{self.file}")
        if not os.path.exists(local_file_path.parent):
            logger.warning(f"Creating folder: {local_file_path.parent}")
            os.makedirs(local_file_path.parent)
        async with concurrency_limit:
            try:
                logger.info(f"Starting download ({self.idx}/{self.total_count}): {self.file}")
                await self.sftp.get(self.file, localpath=f"{self.localdir}{self.file}")
                logger.info(f"Completed download ({self.idx}/{self.total_count}): {self.file}")
                self.completed = True
            except Exception as ex:
                logger.error('Connection likely closed unexpectedly. Restarting the process would likely resolve the issue.')


async def main():
    async with asyncssh.connect(
        config.sftp_host,
        username=config.sftp_username,
        password=config.sftp_password,
        known_hosts=None,
    ) as conn:
        logger.info(f"Session timestamp: {timestamp}")
        async with conn.start_sftp_client() as sftp:
            files = await sftp.glob(f"/{config.remote_folder_to_download}/*")
            logger.info(f"Downloading {len(files)} files with concurrency {concurrency_limit._value}...")
            local_dir = os.path.join(config.local_downloads_root_folder, timestamp) if config.store_files_by_session else config.local_downloads_root_folder
            tasks = [
                DownloadFileTask(
                    sftp=sftp,
                    file=file,
                    localdir=local_dir,
                    idx=i+1,
                    total_count=len(files),
                ) for i, file in enumerate(files)
            ]

            retries = 0
            while len([task for task in tasks if not task.completed]) > 0:
                execs = [task.execute() for task in tasks if not task.completed]
                res = await asyncio.gather(*execs)
                if 0 <= retries <= config.max_batch_retries:
                    print('Retrying failed tasks...')
                    retries += 1

    logger.info('All done')

asyncio.run(main())
