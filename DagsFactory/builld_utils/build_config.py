def build_config(names: list, template_config: str, filepath: str = '.'):
    for agg_name, job_name in names:
        with open(f"{filepath}/{agg_name}_aggregation.json", "w+") as file:
            file.write(template_config.replace("$name", agg_name).replace("$job_name", job_name))
