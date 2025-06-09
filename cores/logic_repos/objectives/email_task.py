from cores.models.jinja import EmailRender


def send_template_email(subject: str,
                        send_to: str | list[str],
                        body_email: str,
                        cc_to: str = None,
                        custom_context: dict = None,
                        attachments: list[str] = None,
                        **kwargs):
    """
    Sends an email using a Jinja2 template for the body content.

    Args:
        subject (str): The subject of the email.
        send_to (str | list[str]): The recipient(s) of the email. Can be a single email address or a list of addresses.
        body_email (str): The path to the email body template.
        cc_to (str, optional): The CC recipients of the email. Default is None.
        custom_context (dict, optional): A dictionary containing custom context variables for rendering the email template. Default is None.
        attachments (list[str], optional): A list of file paths to attach to the email. Default is None.
        **kwargs: Additional arguments to pass to the `EmailRender` class.

    Process:
        - Renders the email body using the `EmailRender` class and the provided template and context.
        - Formats the `send_to` list into a semicolon-separated string if multiple recipients are provided.
        - Sends the email using the `send_email` utility with the rendered body and specified attachments.

    Example:
        send_template_email(
            subject="Monthly Report",
            send_to=["recipient1@example.com", "recipient2@example.com"],
            body_email="templates/monthly_report.template",
            cc_to="manager@example.com",
            custom_context={"month": "January", "year": 2025"},
            attachments=["/path/to/report.pdf"]
        )
    """
    from cores.utils.email_smtp import send_email

    custom_context = custom_context or {}
    render = EmailRender(**kwargs,
                         **custom_context,
                         )
    email_body = render.render_file(body_email)

    if isinstance(send_to, list): send_to = ";".join(send_to)

    data = dict(send_to=send_to,
                msg_body=email_body,
                cc_to=cc_to,
                subject=subject,
                files=attachments)
    # print("[send_template_email] data:", data)

    send_email(**data)


if __name__ == "__main__":
    import textwrap

    test_render = EmailRender(run_date="test-run-date 2222", email_data=[{"col-1": 1212}])

    output = test_render.render_file(textwrap.dedent(
        """
            Dear All,

            List of Users have updated, run_date={{ run_date }}:
            {{ load_table_from_dicts(email_data) }}

            Regards,
            EDW Automation
            """
    ))

    print(output)
