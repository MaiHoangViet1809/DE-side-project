import os
import ast
from diagrams import Diagram, Cluster, Edge
from diagrams.programming.language import Python as Node  # or "Blank" if you prefer


def file_path_to_module_name(file_path: str, root_dir: str) -> str:
    rel_path = os.path.relpath(file_path, start=root_dir)
    if rel_path.endswith(".py"):
        rel_path = rel_path[:-3]
    return rel_path.replace(os.path.sep, ".")


def build_directory_tree(local_modules: list[str]) -> dict:
    """
    Construct a nested dictionary representing directories and their files.
    E.g. { "cores": { "_files": {...}, "utils": { "_files": {...} } } }
    """
    tree = {}
    for mod in local_modules:
        parts = mod.split(".")
        current = tree
        for dir_part in parts[:-1]:
            current.setdefault(dir_part, {})
            current = current[dir_part]
        current.setdefault("_files", set())
        current["_files"].add(parts[-1])
    return tree


def create_clusters_recursively(tree: dict, parent_name: str, node_map: dict):
    """
    Recursively create clusters for directories and nodes for each file.
    'parent_name' is the dotted path to this directory, e.g. 'cores.utils'.
    """
    if parent_name:
        c = Cluster(parent_name)
        cluster_ctx = c
    else:
        cluster_ctx = None

    subdirs = {k: v for k, v in tree.items() if k != "_files"}

    def create_files():
        if "_files" not in tree:
            return
        for file_name in sorted(tree["_files"]):
            dotted_name = file_name if not parent_name else f"{parent_name}.{file_name}"
            # We use the last part as the visible label
            node_map[dotted_name] = Node(file_name)

    def create_subdirs():
        for dir_name in sorted(subdirs.keys()):
            new_parent = dir_name if not parent_name else f"{parent_name}.{dir_name}"
            create_clusters_recursively(subdirs[dir_name], new_parent, node_map)

    if cluster_ctx:
        with cluster_ctx:
            create_files()
            create_subdirs()
    else:
        # top-level
        create_files()
        create_subdirs()


def build_nested_clusters_and_nodes(directory_tree: dict) -> dict:
    """
    Build a node_map { dotted_name: NodeObj } by recursively creating clusters.
    """
    node_map = {}
    create_clusters_recursively(directory_tree, parent_name="", node_map=node_map)
    return node_map


# ----------------------------------------------------
#  Build local dependency dictionary
# ----------------------------------------------------

def build_local_module_map(target_dir: str, project_dir: str, exclude_dirs=None):
    """
    Build a map of absolute file path -> dotted module name, skipping subdirectories in `exclude_dirs`.
    """
    if exclude_dirs is None:
        exclude_dirs = []

    # Convert exclude_dirs to absolute
    exclude_abs = [os.path.join(os.path.abspath(project_dir), e) for e in exclude_dirs]

    file_to_mod = {}
    mod_to_file = {}
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                # Skip if any excluded path is contained in the full_path
                if any(excl in full_path for excl in exclude_abs):
                    continue
                dotted_name = file_path_to_module_name(full_path, project_dir)
                file_to_mod[full_path] = dotted_name
                mod_to_file[dotted_name] = full_path
    return file_to_mod, mod_to_file


def extract_dependencies_with_mapping(target_dir: str, project_dir: str, exclude_dirs=None) -> dict[str, list[str]]:
    """
    Return a dict { local_module: [imported_local_module, ...], ... }
    Also includes references from __init__.py modules if they exist.
    """
    file_to_mod, mod_to_file = build_local_module_map(target_dir, project_dir, exclude_dirs)

    dependencies = {}
    for abs_path, local_mod_name in file_to_mod.items():
        dependencies[local_mod_name] = []

        try:
            with open(abs_path, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=abs_path)

            # Normal import statements
            imports = [
                node.names[0].name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import) and node.names
            ]
            # From-import statements
            from_imports = [
                node.module
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module
            ]

            all_imports = set(imports + from_imports)
            for imp in all_imports:
                if imp in mod_to_file:
                    dependencies[local_mod_name].append(imp)

        except Exception as e:
            print(f"Error parsing {abs_path}: {e}")

    return dependencies


# ----------------------------------------------------
#  Post-processing: Expand references to __init__.py
# ----------------------------------------------------

def expand_init_imports(deps: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    - Identify any module that ends with '.__init__'.
    - Let's call that an 'init' module. We'll gather the modules it imports.
    - For each other module M referencing 'init', remove that reference and
      add all of 'init's dependencies (transitively).
    - Finally, remove the 'init' modules from the dictionary so they won't appear as nodes.
    """

    # 1) Collect the adjacency lists for every init module
    init_map = {}
    for mod, imports in deps.items():
        if mod.endswith(".__init__"):
            init_map[mod] = list(imports)  # copy of the list

    # 2) Expand references in all non-init modules
    for mod, imports in deps.items():
        if mod in init_map:
            # skip if it's an init itself
            continue

        # We'll build a new import list while expanding any init references
        new_imports = []
        for dep in imports:
            if dep in init_map:
                # Expand references: remove dep, add the init's dependencies
                new_imports.extend(init_map[dep])
            else:
                new_imports.append(dep)
        deps[mod] = list(set(new_imports))  # remove duplicates, if any

    # 3) Remove init modules from the dictionary
    for init_mod in init_map.keys():
        deps.pop(init_mod, None)

    return deps


# ----------------------------------------------------
#  Create the nested diagram
# ----------------------------------------------------

def create_nested_cluster_diagram(
    deps: dict[str, list[str]],
    diagram_name="Nested Modules",
    out_filename="nested_deps",
    show_diagram=False
):
    """
    1) Build directory tree from the final keys in `deps`.
    2) Create nested clusters + nodes (no __init__ modules remain).
    3) Draw edges based on the adjacency list.
    """
    all_modules = list(deps.keys())  # dotted names
    directory_tree = build_directory_tree(all_modules)

    with Diagram(
        diagram_name,
        filename=out_filename,
        outformat="png",
        show=show_diagram,
        direction="TB",
        strict=True,
        graph_attr={
            "concentrate": "true",
            "overlap": "false",
            "ranksep": "2.0",
            "nodesep": "0.8",
            "remincross": "true",
            "layout": "dot",
        },
        edge_attr={
            "arrowhead": "normal",
            "arrowsize": "0.8",
        },
    ):
        node_map: dict[str, Node] = {}
        create_clusters_recursively(directory_tree, parent_name="", node_map=node_map)

        # Now connect edges
        for src_mod, imports in deps.items():
            src_node = node_map.get(src_mod)
            for dst_mod in imports:
                dst_node = node_map.get(dst_mod)
                if src_node and dst_node:
                    # Check special keywords
                    if "spark" in dst_mod.lower() :
                        # Heavier edge
                        src_node << Edge(color="red", penwidth="3.0", weight="6") << dst_node

                    elif "mssql" in src_mod.lower() or "mssql" in dst_mod.lower():
                        src_node << Edge(color="blue", penwidth="2.0", weight="3") << dst_node

                    elif "sftp" in dst_mod.lower():
                        src_node << Edge(color="purple", penwidth="2.0", weight="2") << dst_node

                    elif "configs" in dst_mod.lower():
                        src_node << Edge(color="green", penwidth="2.0", weight="2") << dst_node

                    else:
                        # Normal edge
                        src_node << Edge(color="black", penwidth="1.0", weight="1") << dst_node

    print(f"Nested diagram saved to {out_filename}.png")


# ----------------------------------------------------
#  Main
# ----------------------------------------------------
if __name__ == "__main__":
    project_directory = "/Users/maihoangviet/Projects/EDW-KC-Project"
    target_dir = os.path.join(project_directory, "cores")

    # 1) Extract local dependencies, including references to __init__ modules
    raw_deps = extract_dependencies_with_mapping(
        target_dir,
        project_directory,
        exclude_dirs=["cores/deprecated"]
    )

    # 2) Expand references to any __init__.py
    final_deps = expand_init_imports(raw_deps)

    # 4) Build the final diagram
    create_nested_cluster_diagram(
        final_deps,
        diagram_name="EDW-KC Local Python Dependencies",
        out_filename="Project Dependencies",
        show_diagram=False
    )
