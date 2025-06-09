import os
from pathlib import Path
from cores.utils.env_info import Env


def generate_api_reference(base_path, base_output_path="./docs/framework API", include_directories=("cores", "00_mini_tools")):
    list_module_name = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                module_path = (Path(root) / file).relative_to(base_path).as_posix()
                module_name = Path(module_path).parent / Path(module_path).stem
                module_name_dot = module_name.as_posix().replace("/", ".").replace("\\", ".").replace(".py", "")

                parent_module = module_name.as_posix().removesuffix("." + str(file).replace(".py", ""))

                if any(module_path.startswith(m) for m in include_directories) and "deprecated" not in module_name.as_posix():
                    print(os.path.join(root, file), file, module_name, module_path, parent_module, module_name_dot)

                    write_mode = "a" if parent_module not in list_module_name else "w"
                    output_file = f"{base_output_path}/{parent_module}.md"
                    Path(output_file).parent.mkdir(exist_ok=True, parents=True)
                    with open(file=output_file, mode=write_mode) as f:
                        if write_mode == "w":
                            f.write(f"---\n\n")
                            f.write(f"## Module: `{parent_module}`  \n\n")
                            f.write(f"---\n\n")
                            list_module_name.append(parent_module)

                        f.write(f"::: {module_name_dot}\n")


if __name__ == "__main__":
    # Replace 'src' with your source directory
    generate_api_reference("..")
