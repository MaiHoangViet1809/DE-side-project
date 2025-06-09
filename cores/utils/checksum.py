import hashlib

from cores.utils.providers import ProvideBenchmark
from cores.utils.functions import human_readable_size


@ProvideBenchmark
def check_sum(file_path: str, total_bytes: int, read_bytes: int = 8192):
    """
    Compute the checksum (hash) of a file using the Blake2b algorithm. Each time the file's contents change,
    the hash will be updated.

    Args:
        file_path (str): Path to the file to check.
        total_bytes (int): Total size of the file in bytes.
        read_bytes (int, optional): Number of bytes to read per iteration. Defaults to 8192.

    Returns:
        str: The computed hash of the file.

    Examples:
        ```python
        file_hash = check_sum("example.txt", total_bytes=102400)
        print(f"File Hash: {file_hash}")
        ```
    """
    print("[check_sum] checking file::", file_path)
    progress_count = 0

    with open(file_path, "rb") as f:
        file_hash = hashlib.blake2b()
        read_so_far = 0
        while chunk := f.read(read_bytes):
            read_so_far += read_bytes
            file_hash.update(chunk)

            progress = min(100, int(read_so_far / total_bytes * 100))
            if progress >= progress_count * 10:
                print(f"[check_sum] progress: {human_readable_size(read_so_far)}/{human_readable_size(total_bytes)} = {read_so_far/total_bytes:.2%}", )
                progress_count += 1

    return file_hash.hexdigest()
