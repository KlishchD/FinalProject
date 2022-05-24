import json
import os.path
import sys


def replace_parameters(parameters: dict, line: str) -> str:
    """
    Replaces all marks in line form template to real value
    :param parameters: dict (mark, value)
    :param line: line where to replace marks
    :return: line with replaced marks
    """
    for key, value in parameters.items():
        try:
            space_count = line.find(f"${key}")
            indent = "".join([" " for _ in range(space_count)])
            line = line.replace(f"${key}", value.replace("\n", "\n" + indent))
        except:
            pass
    return line


def generate_dag(parameters: dict, templates_dir_path: str) -> None:
    """
    :param parameters: parameters that are used to create dag
    :param templates_dir_path: path to a directory with dags templates
    """
    with open(f"{templates_dir_path}/{parameters['mode']}_template.py") as template:
        with open(parameters["result_filepath"], "w+") as result:
            for line in template:
                result.write(replace_parameters(parameters["dag_parameters"], line))


def get_filepaths_to_process(filepath: str) -> list:
    """
    :param filepath: path from cli
    :return: list of configs' filepaths
    """
    if os.path.isdir(filepath):
        relative_paths = os.listdir(filepath)
        return [filepath + "/" + relative_path for relative_path in relative_paths]
    return [filepath]


def parse_cluster_size(parameters: dict) -> None:
    """
    Substitutes cluster size with appropriate cluster config
    :param parameters: loaded config for dag
    """
    cluster_size = parameters["dag_parameters"]["cluster_config"]
    parameters["dag_parameters"]["cluster_config"] = json.load(open(f"cluster_sizes/{cluster_size}_size.json"))


def cast_all_values_to_string(parameters: dict) -> None:
    for key in parameters:
        if type(parameters[key]) is dict:
            parameters[key] = json.dumps(parameters[key], indent=4)
        else:
            parameters[key] = str(parameters[key])


def __main__():
    filepath = sys.argv[1]
    templates_dir_path = sys.argv[2] if len(sys.argv) > 2 else "."
    for file in get_filepaths_to_process(filepath):
        parameters = json.load(open(file))
        if parameters["mode"] == "prod":
            parse_cluster_size(parameters)
        cast_all_values_to_string(parameters["dag_parameters"])
        generate_dag(parameters, templates_dir_path)


if __name__ == "__main__":
    __main__()
