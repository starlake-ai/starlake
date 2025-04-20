import os

import tempfile

from typing import List

from ai.starlake.job import IStarlakeJob

class StarlakeFargateHelper:
    """A helper class to generate and run a starlake command on AWS Fargate."""
    def __init__(self, job: IStarlakeJob, arguments: list, **kwargs) -> None:
        """Initializes the StarlakeFargateHelper.
        Args:
            job (IStarlakeJob): The Starlake job.
            arguments (list): The arguments of the starlake command to run.
            **kwargs: The optional arguments.
        """
        super().__init__()

        options = job.options
        caller_globals = job.caller_globals

        # aws profile
        self.__profile = kwargs.get("profile", caller_globals.get("aws_profile", job.__class__.get_context_var("aws_profile", "default", options)))
        kwargs.pop("profile", None)
        # aws region
        self.__region = kwargs.get("region", caller_globals.get("aws_region", job.__class__.get_context_var("aws_region", "eu-west-3", options)))
        kwargs.pop("region", None)
        # aws cluster name
        self.__cluster = kwargs.get("cluster", caller_globals.get("aws_cluster_name", job.__class__.get_context_var("aws_cluster_name", None, options)))
        kwargs.pop("cluster", None)
        # aws task private subnets
        subnets = kwargs.get("subnets", caller_globals.get("aws_task_private_subnets", job.__class__.get_context_var("aws_task_private_subnets", [], options)))
        if isinstance(subnets, str):
            self.__subnets = subnets.split(",")
        else:
            self.__subnets = subnets
        kwargs.pop("subnets", None)
        # aws security groups
        security_groups = kwargs.get("security_groups", caller_globals.get("aws_task_security_groups", job.__class__.get_context_var("aws_task_security_groups", [], options)))
        if isinstance(security_groups, str):
            self.__security_groups = security_groups.split(",")
        else:
            self.__security_groups = security_groups
        kwargs.pop("security_groups", None)
        # aws task definition name
        self.__task_definition = kwargs.get("task_definition", caller_globals.get("aws_task_definition_name", job.__class__.get_context_var("aws_task_definition_name", None, options)))
        kwargs.pop("task_definition", None)
        # aws task definition container name
        self.__container_name = kwargs.get("container_name", caller_globals.get("aws_task_definition_container_name", job.__class__.get_context_var("aws_task_definition_container_name", None, options)))
        kwargs.pop("container_name", None)
        # overrides aws container cpu
        self.__cpu: int = int(kwargs.get("cpu", caller_globals.get("cpu", job.__class__.get_context_var("cpu", 1024, options))))
        kwargs.pop("cpu", None)
        # overrides aws container memory
        self.__memory: int = int(kwargs.get("memory", caller_globals.get("memory", job.__class__.get_context_var("memory", 2048, options))))
        kwargs.pop("memory", None)

        sl_env_vars = job.__class__.get_sl_env_vars(options)

        self.__arguments = arguments

        env_vars = dict()
        if arguments is None:
            env_vars.update(sl_env_vars)
        else:
            found = False

            for index, arg in enumerate(arguments):
                if arg == "--options" and arguments.__len__() > index + 1:
                    opts = arguments[index+1]
                    if opts.strip().__len__() > 0:
                        temp = sl_env_vars.copy() # Copy the current sl env variables
                        temp.update({
                            key: value
                            for opt in opts.split(",")
                            if "=" in opt  # Only process valid key=value pairs
                            for key, value in [opt.split("=")]
                        })
                        env_vars.update(temp)
                    else:
                        env_vars.update(sl_env_vars) # Add/overwrite with sl env variables
                    found = True
                    break

            if not found:
                env_vars.update(sl_env_vars) # Add/overwrite with sl env variables

        # overrides aws container environment
        environment = []
        # Convert the env vars to the required format
        for key, value in env_vars.items():
            environment.append({"name": key, "value": value})
        self.__environment = environment

        # aws sdk path
        self.__sdk = kwargs.get("sdk", caller_globals.get("AWS_SDK",job.__class__.get_context_var('AWS_SDK', '/usr/local/aws-cli', options)))


    @property
    def arguments(self) -> list:
        """The arguments of the starlake command to run."""
        return self.__arguments or []

    @arguments.setter
    def arguments(self, value: list):
        """Set the arguments of the starlake command to run."""
        self.__arguments = value

    @property
    def profile(self) -> str:
        """The AWS profile to use."""
        return self.__profile

    @property
    def region(self) -> str:
        """The AWS region to use."""
        return self.__region
    
    @property
    def cluster(self) -> str:
        """The AWS ecs cluster name to use."""
        return self.__cluster
    
    @property
    def subnets(self) -> List[str]:
        """The AWS ecs task private subnets to use."""
        return self.__subnets
    
    @property
    def security_groups(self) -> List[str]:
        """The AWS ecs task security groups to use."""
        return self.__security_groups
    
    @property
    def task_definition(self) -> str:
        """The AWS ecs task definition name to use."""
        return self.__task_definition
    
    @property
    def container_name(self) -> str:
        """The AWS ecs task definition container name to override."""
        return self.__container_name
    
    @property
    def cpu(self) -> int:
        """The AWS ecs task definition container cpu to override."""
        return self.__cpu
    
    @property
    def memory(self) -> int:
        """The AWS ecs task definition container memory to override."""
        return self.__memory
    
    @property
    def environment(self) -> List[dict]:
        """The AWS ecs task definition container environment to override."""
        return self.__environment

    @environment.setter
    def environment(self, value: List[dict]):
        """Set the AWS ecs task definition container environment to override."""
        self.__environment = value

    @property
    def command(self) -> str:
        """The AWS ecs task definition container command to override."""
        return " ".join(self.arguments)

    @property
    def sdk(self) -> str:
        """The AWS sdk path to use."""
        return self.__sdk

    @property
    def overrides(self) -> dict:
        """The AWS ecs task definition overrides."""
        return {
            "containerOverrides": [{
                "name": self.container_name,
                "command": self.arguments,
                "environment": self.environment,
                "cpu": self.cpu,
                "memory": self.memory
            }]
        }

    @property
    def overrides_str(self) -> str:
        """The AWS ecs task definition overrides string."""
        import json
        return json.dumps(self.overrides)

    def generate_script(self) -> str:
        """Generate the script to run the starlake command.
        Returns:
            str: The absolute path to the generated script file.
        """
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
            tmp_file_path = tmp_file.name
            tmp_path = os.path.dirname(tmp_file_path)
            print("Using temporary directory: %s" % tmp_path)

            # Lancer la tâche et capturer le taskArn
            task_arn = (
                f"{self.sdk}/aws ecs run-task \\\n"
                f"\t--profile {self.profile} \\\n"
                f"\t--region {self.region} \\\n"
                f"\t--cluster {self.cluster} \\\n"
                "\t--launch-type FARGATE \\\n"
                f"\t--network-configuration 'awsvpcConfiguration={{subnets={self.subnets},securityGroups={self.security_groups},assignPublicIp=\"DISABLED\"}}' \\\n"
                f"\t--task-definition {self.task_definition} \\\n"
                f"\t--overrides '{self.overrides_str}' \\\n"
                "\t--query 'tasks[0].taskArn' \\\n"
                "\t--output text"
            )
            tmp_file.write(f"task_arn=$({task_arn})\n")
            tmp_file.write("if [ -z \"$task_arn\" ]; then\n")
            tmp_file.write("\techo \"Failed to start task\"\n")
            tmp_file.write("\texit 1\n")
            tmp_file.write("fi\n")
            tmp_file.write("echo \"Task ARN: $task_arn\"\n")
            # Vérifier l'état de la tâche en boucle
            tmp_file.write("status=\"\"\n")
            tmp_file.write("while [ \"$status\" != \"STOPPED\" ]; do\n")
            tmp_file.write("\techo \"Checking task status...\"\n")
            tmp_file.write(f"\tstatus=$(aws ecs describe-tasks \\\n\t--profile {self.profile} \\\n\t--region {self.region} \\\n\t--cluster {self.cluster} \\\n\t--tasks $task_arn \\\n\t--query 'tasks[0].lastStatus' \\\n\t--output text)\n")
            tmp_file.write("\techo \"Current status: $status\"\n")
            tmp_file.write("\tsleep 10\n")
            tmp_file.write("done\n")
            # Récupérer les logs ou le code de retour final
            tmp_file.write(f"exit_code=$(aws ecs describe-tasks \\\n\t--profile {self.profile} \\\n\t--region {self.region} \\\n\t--cluster {self.cluster} \\\n\t--tasks $task_arn \\\n\t--query 'tasks[0].containers[0].exitCode' \\\n\t--output text)\n")
            tmp_file.write("echo \"Task finished with exit code: $exit_code\"\n")
            tmp_file.write("exit $exit_code")
            tmp_file.flush()

            tmp_file = os.path.basename(tmp_file_path)
            print(f"Temporary script location: {tmp_file}")

            return tmp_file_path

    def run_task(self):
        """Run the AWS ecs run-task command and wait for the result."""
        import subprocess
        import time

        start_time = time.perf_counter()

        script_path = self.generate_script()

        process = subprocess.Popen(
            ["bash", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True  # Assure que la sortie est traitée comme une chaîne et non des bytes
        )

        # Lecture en temps réel des sorties
        for line in iter(process.stdout.readline, ""):
            print(line, end="")  # Affiche sans ajout de nouvelle ligne supplémentaire

        # Attendre la fin du processus
        process.stdout.close()
        process.stderr.close()
        process.wait()

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        minutes, seconds = divmod(elapsed_time, 60)

        print(f"Execution time {script_path} : {int(minutes)} min {seconds:.2f} sec")

        return process.returncode
