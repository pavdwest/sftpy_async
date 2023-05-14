import asyncio
import os
from pathlib import Path
from datetime import datetime

from logger import get_logger
import config
import asyncssh


timestamp = datetime.now().strftime('%Y%m%d_%H%M%S%f')
logger = get_logger(filename=timestamp)
concurrency_limit = asyncio.Semaphore(config.batch_size)


async def download_file(sftp, file: str, localdir: str, idx: int, total_count: int):
    # TODO: Delete after download
    
    local_file_path = Path(f"{localdir}/{file}")
    if not os.path.exists(local_file_path.parent):
        logger.warning(f"Creating folder: {local_file_path.parent}")
        os.makedirs(local_file_path.parent)

    async with concurrency_limit:
        try:
            logger.info(f"Starting download ({idx}/{total_count}): {file}")
            await sftp.get(file, localpath=f"{localdir}{file}")
            logger.info(f"Completed download ({idx}/{total_count}): {file}")
        except Exception as ex:
            logger.error("Connection likely closed unexpectedly. Restarting the process would likely resolve the issue.")
            raise ex


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
            logger.info(f"Downloading {len(files)} files...")
            local_dir = os.path.join(config.local_downloads_root_folder, timestamp) if config.store_files_by_session else config.local_downloads_root_folder
            tasks = [download_file(sftp, file, localdir=local_dir, idx=i+1, total_count=len(files)) for i, file in enumerate(files)]
            await asyncio.gather(*tasks)

    logger.info('All done')

asyncio.run(main())
