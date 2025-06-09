from typing import List

DEFAULT_CSS_STYLE = """
<style>
<!--
/* Font Definitions */
@font-face
    {font-family:\"Cambria Math\";
    panose-1:2 4 5 3 5 4 6 3 2 4;}
@font-face
    {font-family:Calibri;
    panose-1:2 15 5 2 2 2 4 3 2 4;}
/* Style Definitions */
p.MsoNormal, li.MsoNormal, div.MsoNormal
    {margin:0cm;
    margin-bottom:.0001pt;
    font-size:11.0pt;
    font-family:\"Calibri\",sans-serif;
    mso-fareast-language:EN-US;}
a:link, span.MsoHyperlink
    {mso-style-priority:99;
    color:#0563C1;
    text-decoration:underline;}
a:visited, span.MsoHyperlinkFollowed
    {mso-style-priority:99;
    color:#954F72;
    text-decoration:underline;}
span.EmailStyle17
    {mso-style-type:personal-compose;
    font-family:\"Calibri\",sans-serif;
    color:windowtext;}
.MsoChpDefault
    {mso-style-type:export-only;
    font-family:\"Calibri\",sans-serif;
    mso-fareast-language:EN-US;}
@page WordSection1
    {size:612.0pt 792.0pt;
    margin:72.0pt 72.0pt 72.0pt 72.0pt;}
div.WordSection1
    {page:WordSection1;}
--></style>
"""


def wrap_html_body(body_html: str, css_style_block: str = None) -> str:
    """
    Wrap an email body in a formatted HTML structure suitable for Outlook styling.

    Args:
        body_html (str): The email body content in HTML format.
        css_style_block (str, optional): Custom CSS style block to include. Defaults to None.

    Returns:
        str: The wrapped HTML body.

    Examples:
        ```python
        html_body = wrap_html_body("<p>Hello, World!</p>", "<style>p { color: red; }</style>")
        print(html_body)
        ```
    """
    msg_body = "<html xmlns:v=\\\"urn:schemas-microsoft-com:vml\\\" xmlns:o=\\\"urn:schemas-microsoft-com:office:office\\\" "
    msg_body += "xmlns:w=\\\"urn:schemas-microsoft-com:office:word\\\" xmlns:x=\\\"urn:schemas-microsoft-com:office:excel\\\" "
    msg_body += "xmlns:m=\\\"http://schemas.microsoft.com/office/2004/12/omml\\\" xmlns=\\\"http://www.w3.org/TR/REC-html40\\\">"
    msg_body += "<head><META HTTP-EQUIV=\\\"Content-Type\\\" CONTENT=\\\"text/html; charset=us-ascii\\\"><meta name=Generator content=\\\"Microsoft Word 15 (filtered medium)\\\">"
    if css_style_block: msg_body += css_style_block
    msg_body += "<!--[if gte mso 9]><xml><o:shapedefaults v:ext=\\\"edit\\\" spidmax=\\\"1026\\\" /></xml><![endif]-->"
    msg_body += "<!--[if gte mso 9]><xml><o:shapelayout v:ext=\\\"edit\\\"><o:idmap v:ext=\\\"edit\\\" data=\\\"1\\\" /></o:shapelayout></xml><![endif]-->"
    msg_body += "</head>\n"
    msg_body += "<body><div class=WordSection1>"
    msg_body += body_html
    msg_body += "</div></body></html>"
    return msg_body


def list_of_dict_to_html(data: List[dict]):
    """
    Convert a list of dictionaries to an HTML table.

    Args:
        data (List[dict]): List of dictionaries (records).

    Returns:
        str: HTML string representing the table.

    Examples:
        ```python
        records = [
            {"Name": "John", "Status": "success"},
            {"Name": "Doe", "Status": "failed"}
        ]
        html_table = list_of_dict_to_html(records)
        print(html_table)
        ```
    """

    header_table = None
    body_table = None

    if len(data) == 0:
        return ""

    for d in data:
        list_key = [*d]
        list_value = [*d.values()]

        prepare_tag = '<td nowrap valign=bottom style=\"border-top:solid windowtext 1.0pt;border-left:solid white 1.0pt;border-bottom:solid windowtext 1.0pt;border-right:none;background:black;padding:0cm 5.4pt 0cm 5.4pt;\">'
        prepare_tag += '<p class=MsoNormal><b><span>'
        if header_table is None:
            header_table = "<tr style=\"background:black;font-size:10.0pt;color:white\">"
            header_table += ''.join([prepare_tag + str(k) + '<o:p></o:p></span></b></p></td>\n' for k in list_key]) + "</tr>"

        if "Run_Spark" in list_value:
            prepare_tag = '<td nowrap valign=bottom style=\"background:yellow;padding:0cm 5.4pt 0cm 5.4pt;height:15.0pt\"><p class=MsoNormal><span style=\"color:{.color};mso-fareast-language:EN-GB\">'
        else:
            prepare_tag = '<td nowrap valign=bottom style=\"padding:0cm 5.4pt 0cm 5.4pt;height:15.0pt\"><p class=MsoNormal><span style=\"color:{.color};mso-fareast-language:EN-GB\">'

        def get_color(v: str):
            if v == "failed":
                return "red"
            elif v == "success":
                return "#00B050"
            else:
                return "black"

        def get_bold(val: str):
            if val in ("failed", "success"):
                return "<b>" + val + "</b>"
            else:
                return val

        len_acc = 0

        def generate_row(k: str):
            nonlocal len_acc
            threshold = 500
            result = prepare_tag.replace("{.color}", get_color(str(k))) + get_bold(str(k)) + ' <o:p></o:p></span></p></td>'
            len_acc += len(result)
            if len_acc >= threshold:
                result += '\n'
                len_acc = 0
            return result

        if body_table is None:
            body_table = "<tr style=\"font-size:12px\">"
            body_table += ''.join(map(generate_row, list_value)) + "</tr>\n"
        else:
            body_table += "<tr style=\"font-size:12px\">"
            body_table += ''.join(map(generate_row, list_value)) + "</tr>\n"

    return "<table class=MsoNormalTable border=1 cellspacing=0 cellpadding=1 style=\"border-collapse:collapse\">" + header_table + body_table + "</table>"
