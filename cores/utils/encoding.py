import chardet
from pathlib import Path


def predict_encoding(file_path: str, n_lines: int = 100) -> str:
    """
    Predict the encoding of a file using `chardet`.

    Args:
        file_path (str): The path to the file to examine.
        n_lines (int, optional): Number of lines to analyze. Defaults to 100.

    Returns:
        str: The predicted encoding of the file.

    Examples:
        ```python
        encoding = predict_encoding("example.txt", n_lines=50)
        print(f"Detected encoding: {encoding}")
        ```
    """
    # Open the file as binary data
    with Path(file_path).open('rb') as f:
        # Join binary lines for specified number of lines
        rawdata = b''.join([f.readline() for _ in range(n_lines)])
    return chardet.detect(rawdata)['encoding']
