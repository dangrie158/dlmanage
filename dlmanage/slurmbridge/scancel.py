from __future__ import annotations

import asyncio
import shutil
from typing import Optional, Tuple

from dlmanage.slurmbridge.cliobject import SlurmObjectException


SCANCEL_PATH = shutil.which("scancel")


class SlurmCancelError(SlurmObjectException):
    def __str__(self) -> str:
        return f"{self.args[1]}"


class SlurmCancelObject:
    @classmethod
    async def _scancel_execute(
        cls, job_id: str, signal: Optional[str] = None
    ) -> Tuple[int | None, str]:
        if SCANCEL_PATH is None:
            raise ImportError("scancel could not be found in path.")

        signal_parameters = []
        if signal is not None:
            signal_parameters = ["--signal", signal]

        process = await asyncio.create_subprocess_exec(
            SCANCEL_PATH,
            *signal_parameters,
            job_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        process_output = (stderr if len(stderr) > 0 else stdout).decode("ascii")
        if process.returncode != 0:
            raise SlurmCancelError(cls, process_output)

        return process.returncode, process_output
