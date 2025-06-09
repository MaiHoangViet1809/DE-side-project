
def human_readable_size(size, decimal_places=2):
    """
    Convert a file size (in bytes) to a human-readable string format.

    Args:
        size (float): File size in bytes to convert.
        decimal_places (int, optional): Number of decimal places in the result. Defaults to 2.

    Returns:
        str: Human-readable file size as a string.

    Examples:
        ```python
        print(human_readable_size(1024))        # Output: "1.00 KiB"
        print(human_readable_size(1048576))    # Output: "1.00 MiB"
        print(human_readable_size(123456789))  # Output: "117.74 MiB"
        ```
    """
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"