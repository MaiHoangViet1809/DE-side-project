import httpx
import tarfile
import os
from document.config import DEFAULT_LOCATION, DEFAULT_LOCATION_CSS


def get_package(package_name):
    folder_name = package_name.replace('/', '_')

    with httpx.Client() as client:
        res = client.get(f"https://registry.npmjs.org/{package_name}/latest")
        tarball_url = res.json()["dist"]["tarball"]

        res = client.get(tarball_url)

        file_path = f"/tmp/{folder_name}.tgz"
        with open(file_path, "wb") as f:
            f.write(res.content)
        with tarfile.open(file_path) as f:
            f.extractall(f"./www/js/{package_name}")

        os.remove(file_path) if os.path.exists(file_path) else print("file tar not found")


def synchronize_script(url, save_location: str = DEFAULT_LOCATION):
    # Get the last part of the URL as the filename
    filename = url.split("/")[-1]
    file_path = os.path.join(save_location, filename)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return "\n".join(f.readlines())

    # Send an HTTP GET request to download the JavaScript library
    with httpx.Client(timeout=300) as client:
        response = client.get(url, follow_redirects=True)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Save the JavaScript code to the file

            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"JavaScript library downloaded and saved to: {file_path}")

            # reopen saved file and read content, return it
            with open(file_path, "r") as f:
                return "\n".join(f.readlines())
        else:
            print(f"Failed to download the JavaScript library. Status code: {response.status_code}")
            raise Exception("Failed to download the JavaScript library")


if __name__ == "__main__":
    # list_packages = ["@tiptap/vue-3", "@tiptap/pm", "@tiptap/starter-kit", "@tiptap/extension-bubble-menu"]
    list_packages = [
        # "@tinymce/tinymce-vue",
        # "@tinymce/tinymce",
        # "@mdi/js",
        "terser"
    ]
    for package in list_packages:
        get_package(package_name=package)