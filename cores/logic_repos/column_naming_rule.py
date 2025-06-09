import re


def p_column_name_enforce(name: str):
    """
    Enforces a standardized naming convention for column names.

    Rules Applied:
        - Replace comparison symbols (`>`, `<`, `>=`, `<=`) with equivalent words (`gt_`, `lt_`, `ge_`, `le_`).
        - Replace all non-alphanumeric characters with underscores (`_`).
        - Prefix column names starting with digits with "C_".
        - Remove leading underscores (`_`) from column names.
        - Remove trailing underscores (`_`) from column names.
        - Replace multiple consecutive underscores with a single underscore.
        - Convert Vietnamese characters to their ASCII equivalents.
        - Convert the column name to uppercase.

    Args:
        name (str): The original column name.

    Returns:
        str: The standardized column name.

    Example:
        >>> p_column_name_enforce("1_column>name")
        'C_1_COLUMN_GT_NAME'
    """
    rule_list = [
        # comparison symbols replace with equivalent word
        lambda x: x.replace(">", "gt_").replace("<", "lt_").replace(">=", "ge_").replace("<=", "le_"),

        # all non-alphanumeric will be replaced by underscore
        lambda x: re.sub(pattern=r'[^\w]+', repl="_", string=x),

        # if name start with digit then add "C" to it
        lambda x: "C_" + x if re.search(pattern=r'^([\d])+', string=x) else x,

        # if name start with underscore, remove it
        lambda x: x[1:] if str(x).startswith("_") else x,

        # if name end with underscore, remove it
        lambda x: x[:-1] if str(x).endswith("_") else x,

        # if name contains more than one underscore in sequence, replace to single underscore
        lambda x: re.sub('_+', '_', x),

        # remove vietnamese char to ASCII
        no_accent_vietnamese,

        # KEEP THIS FINAL RULE: name should be uppercase
        str.upper,
    ]

    # apply rule
    for rule in rule_list:
        name = rule(str(name))

    return name


def no_accent_vietnamese(s):
    """
    Converts Vietnamese characters in a string to their ASCII equivalents.

    Args:
        s (str): The input string containing Vietnamese characters.

    Returns:
        str: The string with Vietnamese characters replaced by their ASCII equivalents.

    Example:
        >>> no_accent_vietnamese("điểm số")
        'diem so'
    """
    s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(r'[ìíịỉĩ]', 'i', s)
    s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(r'[Đ]', 'D', s)
    s = re.sub(r'[đ]', 'd', s)
    return s
