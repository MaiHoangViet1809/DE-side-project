from contextlib import contextmanager

from paramiko import transport, SFTPClient as _SFTPClient, common
from paramiko.transport import Transport as _Transport

from airflow.providers.sftp.hooks.sftp import SFTPHook as _SFTPHook

from cores.utils.functions import human_readable_size


transport.Transport._preferred_kex = (
        'ecdh-sha2-nistp256',
        'ecdh-sha2-nistp384',
        'ecdh-sha2-nistp521',
        # 'diffie-hellman-group16-sha512',  # disable
        'diffie-hellman-group-exchange-sha256',
        'diffie-hellman-group14-sha256',
        'diffie-hellman-group-exchange-sha1',
        'diffie-hellman-group14-sha1',
        'diffie-hellman-group1-sha1',
)


def progress_callback(curr: int, total: int, elapsed_time=None):
    """
    Callback function to track progress during file transfer.

    Args:
        curr (int): Current number of bytes transferred.
        total (int): Total number of bytes to transfer.
        elapsed_time (float, optional): Elapsed time in seconds. Default is None.

    Returns:
        None
    """
    cal = curr / total
    if any([x for x in range(0, 101, 10) if x - 1 <= round(cal * 100) <= x * 10 + 1]) and round(cal * 1000) % 10 == 0:
        duration_prefix = f"duration: {elapsed_time:,.2f} sec" if elapsed_time else ""
        print(duration_prefix + f" progress: {human_readable_size(curr)}/{human_readable_size(total)} ({cal:.2%}) speed: {human_readable_size(curr / elapsed_time)}/s")


class _SFTPClientExtend(_SFTPClient):
    """
    Extends the Paramiko SFTP client to include additional functionality, such as transfer progress tracking.

    Methods:
        - `_transfer_with_callback(reader, writer, file_size, callback=None, read_bytes=32 * 1024)`:
            Transfers data with an optional callback for progress updates.
    """

    @staticmethod
    def _transfer_with_callback(reader, writer, file_size, callback=None, read_bytes: int = 32 * 1024): # 32768
        """
        Transfers data with an optional progress callback.

        Args:
            reader: Input stream to read data from.
            writer: Output stream to write data to.
            file_size (int): Total file size in bytes.
            callback (callable, optional): Progress callback function.
            read_bytes (int): Number of bytes to read at a time. Default is 32 KB.

        Returns:
            int: Total size of the data transferred.
        """
        import time
        size = 0
        start_time = time.time_ns()

        try:
            print("[_SFTPClientExtend._transfer_with_callback] start transfer")
            while data := reader.read(read_bytes):
                writer.write(data)
                size += len(data)
                if callback:
                    elapsed_time = (time.time_ns() - start_time) / 1e9
                    callback(size, file_size, elapsed_time=elapsed_time)
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise e
        return size


class SFTPHook(_SFTPHook):
    """
    Extended SFTP Hook to provide enhanced file transfer capabilities and customization options.

    Attributes:
        default_conn_name (str): Default connection ID for SFTP. Default is "sftp_default".

    Methods:
        get_conn(conn_id=None): Retrieves an SFTP connection.
        create_client(host, port, user, passwd, **kwargs): Creates and returns an SFTP client.
        close_conn: Closes the SFTP connection.
        __call__(conn_id=None): Sets or updates the SFTP connection ID.
        __enter__: Context manager entry point for the SFTP client.
        __exit__: Context manager exit point.
        get_dir_info(path, conn_id=None): Retrieves directory information as a DataFrame.
        download_with_contextmanager(full_path): Context manager for downloading files with optional cleanup.
    """
    default_conn_name = "sftp_default"

    def __init__(self, conn_id: str = None, *args, **kwargs):
        """
        Initializes the SFTPHook.

        Args:
            conn_id (str, optional): Connection ID for SFTP. Default is None.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.transport = None
        super().__init__(*args, **kwargs)
        if conn_id:
            self.ssh_conn_id = conn_id

    def get_conn(self, conn_id: str = None) -> _SFTPClientExtend:
        """
        Retrieves an SFTP connection.

        Args:
            conn_id (str, optional): Connection ID for SFTP. Default is None.

        Returns:
            _SFTPClientExtend: Extended SFTP client instance.
        """
        if self.conn is None:
            conn = self.get_connection(conn_id or self.ssh_conn_id)
            extra_options = conn.extra_dejson if conn.extra else {}
            self.conn = self.create_client(host=conn.host,
                                           port=conn.port,
                                           user=conn.login,
                                           passwd=conn.password,
                                           **extra_options)
        return self.conn

    def create_client(self, host, port, user, passwd, **kwargs):
        """
        Creates and returns an SFTP client with custom transport settings.

        Args:
            host (str): Hostname of the SFTP server.
            port (int): Port of the SFTP server.
            user (str): Username for authentication.
            passwd (str): Password for authentication.
            **kwargs: Additional transport options.

        Returns:
            _SFTPClientExtend: Configured SFTP client.
        """
        # Open a transport
        self.transport = _Transport((host, port))
        self.transport._preferred_kex = (
                'ecdh-sha2-nistp256',
                'ecdh-sha2-nistp384',
                'ecdh-sha2-nistp521',
                # 'diffie-hellman-group16-sha512',  # disable
                'diffie-hellman-group-exchange-sha256',
                'diffie-hellman-group14-sha256',
                'diffie-hellman-group-exchange-sha1',
                'diffie-hellman-group14-sha1',
                'diffie-hellman-group1-sha1',
        )
        self.transport.packetizer.REKEY_BYTES = pow(2, 40)  # 1TB max, this is a security degradation!
        self.transport.packetizer.REKEY_PACKETS = pow(2, 40)  # 1TB max, this is a security degradation!
        self.transport.default_window_size = common.MAX_WINDOW_SIZE // 2

        self.transport.connect(None, user, passwd)

        sftp = _SFTPClientExtend.from_transport(self.transport)
        sftp.get_channel().in_window_size = 2097152
        sftp.get_channel().out_window_size = 2097152
        sftp.get_channel().in_max_packet_size = 2097152
        sftp.get_channel().out_max_packet_size = 2097152
        return sftp

    def close_conn(self) -> None:
        """
        Closes the SFTP connection.

        Returns:
            None
        """
        """Closes the SFTP connection."""
        if isinstance(self.conn, _SFTPClient):
            self.conn.close()
            self.conn = None

        if isinstance(self.transport, _Transport):
            self.transport.close()
            self.transport = None

    def __call__(self, conn_id=None):
        """
        Updates the SFTP connection ID.

        Args:
            conn_id (str, optional): New connection ID. Default is None.

        Returns:
            SFTPHook: The current instance.
        """
        if conn_id:
            self.ssh_conn_id = conn_id
        return self

    def __enter__(self) -> _SFTPClientExtend:
        """
        Context manager entry point.

        Returns:
            _SFTPClientExtend: The SFTP client instance.
        """
        return self.get_conn()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Traceback, if any.
        """
        self.close_conn()

    def get_dir_info(self, path: str, conn_id=None):
        """
        Retrieves directory information and metadata as a DataFrame.

        Args:
            path (str): Directory path to fetch information for.
            conn_id (str, optional): Connection ID for SFTP. Default is None.

        Returns:
            DataFrame: Directory information with file metadata.
        """
        from pandas import DataFrame, to_datetime
        with self.__call__(conn_id):
            data = [attr.__dict__ for attr in self.conn.listdir_attr(path=path)]
            df = (
                DataFrame(data)
                .sort_values("st_mtime", ascending=False)
                .assign(
                    last_modified_dt=lambda x: to_datetime(x['st_mtime'], unit='s'),
                    last_access_dt=lambda x: to_datetime(x['st_atime'], unit='s'),
                    file_size=lambda x: x['st_size'].apply(human_readable_size),
                    parent_path=path,
                )
                [["parent_path", "filename", "last_modified_dt", "last_access_dt", "file_size", "st_size", "st_mtime", "st_atime", "st_mode", "attr", "longname"]]
            )
        return df

    @contextmanager
    def download_with_contextmanager(self, full_path: str):
        """
        Context manager for downloading files with optional cleanup.

        Args:
            full_path (str): Full path of the file to download.

        Yields:
            Path: The downloaded file path and a flag indicating whether it was newly downloaded.
        """
        from pathlib import Path
        from cores.utils.configs import FrameworkConfigs as cfg

        if cfg.Ingestion.ONEDRIVE_BACKUP_LOCATION in full_path:
            yield Path(full_path), False

        else:
            temp_path = f"/tmp/{Path(full_path).name}"
            temp_file = None

            with self as client:
                print(f"[download_with_contextmanager] loading file: {full_path}")
                try:
                    client.get(remotepath=full_path, localpath=temp_path, callback=progress_callback)
                    temp_file = Path(temp_path)
                    yield temp_file, True
                finally:
                    print("[download_with_contextmanager] remove temp file")
                    if temp_file:
                        temp_file.unlink(missing_ok=True)


if __name__ == "__main__":
    from cores.utils.debug import print_table

    hook = SFTPHook()

    # check function
    df_check = hook.get_dir_info("VN/KCVNBiz")
    print_table(df_check, 100)

