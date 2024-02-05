from ai.starlake.job.starlake_options import StarlakeOptions

class StarlakeSparkExecutorConfig:
    def __init__(self, memory: str, cores: int, instances: int):
        super().__init__()
        self.memory = memory
        self.cores = cores
        self.instances = instances

    def __config__(self):
        return {
            'spark.executor.memory': self.memory,
            'spark.executor.cores': str(self.cores),
            'spark.executor.instances': str(self.instances)
        }
 
    def __str__(self):
        return str(self.__dict__)

class StarlakeSparkConfig(StarlakeSparkExecutorConfig):
    def __init__(self, memory: str, cores: int, instances: int, cls_options: StarlakeOptions, options: dict, **kwargs):
        super().__init__(
            memory = cls_options.__class__.get_context_var(var_name='spark_executor_memory', default_value='11g', options={} if not options else options) if not memory else memory,
            cores = cls_options.__class__.get_context_var(var_name='spark_executor_cores', default_value=4, options={} if not options else options) if not cores else cores,
            instances = cls_options.__class__.get_context_var(var_name='spark_executor_instances', default_value=1, options={} if not options else options) if not instances else instances
        )
        self.spark_properties = kwargs

    def __config__(self):
        return dict(
            super().__config__(),
            **self.spark_properties
        )
